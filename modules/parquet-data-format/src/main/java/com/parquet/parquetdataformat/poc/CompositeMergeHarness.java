/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.poc;

import com.parquet.parquetdataformat.bridge.NativeSettings;
import com.parquet.parquetdataformat.bridge.RustBridge;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.index.engine.exec.merge.RowIdMapping;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

class MergeableIndexSortWriter extends IndexWriter {
    public MergeableIndexSortWriter(Directory d, IndexWriterConfig conf) throws IOException {
        super(d, conf);
    }

    protected void doMerge(MergePolicy.OneMerge merge) throws IOException {
        super.merge(merge);
    }
}

/**
 * Composite merge harness: merges data produced by {@link CompositeIngestHarness}.
 *
 * <p>Flow (mirrors the real {@code MergeHandler.doMerge}):
 * <ol>
 *   <li><b>Parquet merge (primary)</b>: calls {@code RustBridge.mergeParquetFilesInRust}
 *       on the 5 parquet files → produces a single merged parquet file and a
 *       {@link RowIdMapping} that maps (fileId, oldRowId) → newRowId.</li>
 *   <li><b>Lucene merge (secondary)</b>: uses the RowIdMapping from step 1 to reorder
 *       Lucene docs via {@code CustomOneMerge.reorder()} → produces a single merged
 *       Lucene segment with rows in the same order as the merged parquet file.</li>
 * </ol>
 *
 * <p>After merge, both the merged Parquet file and the merged Lucene segment contain
 * the same rows in the same order, verifiable with {@link CompositeDataVerifier}.
 *
 * <p><b>Usage:</b>
 * {@code java CompositeMergeHarness [dataDir]}
 *
 * <p>Defaults: dataDir=/Users/darsaga/lucene-index
 */
public class CompositeMergeHarness {

    private static final String ROW_ID = "___row_id";
    private static final String INDEX_NAME = "composite_bench";

    public static void main(String[] args) throws Exception {
        Path dataDir = args.length > 0
            ? Path.of(args[0])
            : Path.of("/Users/darsaga/lucene-index");

        Path luceneDir = dataDir.resolve("lucene");
        Path parquetDir = dataDir.resolve("parquet");
        Path metaFile = dataDir.resolve("ingest_meta.properties");

        // Read metadata
        Properties props = new Properties();
        props.load(Files.newBufferedReader(metaFile));
        int numSegments = Integer.parseInt(props.getProperty("numSegments"));
        int rowsPerSegment = Integer.parseInt(props.getProperty("rowsPerSegment"));
        long totalRows = Long.parseLong(props.getProperty("totalRows"));

        System.out.printf("Input: %d segments, %d rows/segment, %d total rows%n",
            numSegments, rowsPerSegment, totalRows);

        pushDefaultRustSettings();

        // Discover parquet files sorted by generation
        List<Path> parquetFiles;
        try (Stream<Path> s = Files.list(parquetDir)) {
            parquetFiles = s.filter(p -> p.toString().endsWith(".parquet"))
                .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                .toList();
        }
        System.out.printf("Parquet files: %d%n", parquetFiles.size());

        // ========== Phase 1: Parquet merge (primary) ==========
        System.out.println("\n===== Phase 1: Parquet merge (primary — Rust) =====");

        Path mergedParquetDir = dataDir.resolve("parquet_merged");
        Files.createDirectories(mergedParquetDir);
        String mergedParquetFile = mergedParquetDir.resolve(
            "_parquet_file_generation_merged_99.parquet").toString();

        long p1Start = System.nanoTime();
        RowIdMapping rowIdMapping = RustBridge.mergeParquetFilesInRust(
            parquetFiles, mergedParquetFile, INDEX_NAME);
        long p1Ms = (System.nanoTime() - p1Start) / 1_000_000;

        long mergedParquetSize = Files.exists(Path.of(mergedParquetFile))
            ? Files.size(Path.of(mergedParquetFile)) : 0;

        System.out.printf("  Parquet merge: %d ms%n", p1Ms);
        System.out.printf("  Merged file:   %s (%d MB)%n",
            mergedParquetFile, mergedParquetSize / (1024 * 1024));
        System.out.printf("  RowIdMapping:  %d entries, %d files, outputFileId=%s%n",
            rowIdMapping.size(), rowIdMapping.getFileOffsets().size(), rowIdMapping.getFileId());

        // Print RowIdMapping file offsets for debugging
        System.out.println("  File offsets:");
        rowIdMapping.getFileOffsets().forEach((fileId, offset) ->
            System.out.printf("    fileId=%s, offset=%d, size=%d%n",
                fileId, offset, rowIdMapping.getFileSize(fileId)));

        // ========== Phase 2: Lucene merge (secondary — using RowIdMapping) ==========
        System.out.println("\n===== Phase 2: Lucene merge (secondary — RowIdMapping reorder) =====");

        Path mergedLuceneDir = dataDir.resolve("lucene_merged");
        Files.createDirectories(mergedLuceneDir);

        long p2Start = System.nanoTime();

        try (Directory srcDir = NIOFSDirectory.open(luceneDir);
             Directory dstDir = NIOFSDirectory.open(mergedLuceneDir)) {

            // Open a CustomIndexWriter on the merged output directory
            IndexWriterConfig iwc = new IndexWriterConfig();
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            iwc.setMergeScheduler(new SerialMergeScheduler());

            try (MergeableIndexSortWriter writer = new MergeableIndexSortWriter(dstDir, iwc)) {
                // Copy all segments from source into the merge directory
                writer.addIndexes(srcDir);
                writer.commit();

                // Collect all segments
                SegmentInfos segInfos = SegmentInfos.readLatestCommit(dstDir);
                List<SegmentCommitInfo> allSegments = new ArrayList<>(segInfos.asList());

                System.out.printf("  Segments before merge: %d%n", allSegments.size());
                for (SegmentCommitInfo sci : allSegments) {
                    System.out.printf("    segment=%s, maxDoc=%d%n",
                        sci.info.name, sci.info.maxDoc());
                }

                // The RowIdMapping from Rust uses file IDs extracted from parquet filenames.
                // extract_file_id("_parquet_file_generation_0.parquet") = "0"
                // Our Lucene segments are in the same order (segment 0 = file "0", etc.)
                // But CustomOneMerge.reorder() looks up by writer_generation attribute,
                // which we don't have. So we build a remapped RowIdMapping that uses
                // segment index as the key, matching the segment order.
                RowIdMapping luceneMapping = remapForLuceneSegments(
                    rowIdMapping, allSegments, numSegments, rowsPerSegment);

                // Execute merge with RowIdMapping reorder
                long mergeStart = System.nanoTime();
                SegmentOrderedOneMerge oneMerge = new SegmentOrderedOneMerge(
                    allSegments, luceneMapping);
                writer.doMerge(oneMerge);
                writer.commit();
                long mergeMs = (System.nanoTime() - mergeStart) / 1_000_000;

                System.out.printf("  Lucene merge: %d ms%n", mergeMs);

                // Verify result
                SegmentInfos afterMerge = SegmentInfos.readLatestCommit(dstDir);
                System.out.printf("  Segments after merge: %d%n", afterMerge.size());
                for (SegmentCommitInfo sci : afterMerge) {
                    System.out.printf("    segment=%s, maxDoc=%d%n",
                        sci.info.name, sci.info.maxDoc());
                }

                try (DirectoryReader reader = DirectoryReader.open(dstDir)) {
                    System.out.printf("  Total docs (all segments): %d%n", reader.numDocs());

                    // The merged segment is the last one (largest)
                    var leaves = reader.leaves();
                    var mergedLeaf = leaves.stream()
                        .max(Comparator.comparingInt(l -> l.reader().maxDoc()))
                        .orElseThrow();
                    System.out.printf("  Merged segment docs: %d%n", mergedLeaf.reader().maxDoc());

                    // Sample first 10 row IDs from merged segment
                    SortedNumericDocValues rowIdDV = mergedLeaf.reader().getSortedNumericDocValues(ROW_ID);
                    System.out.print("  First 10 row IDs in merged segment: ");
                    for (int i = 0; i < Math.min(10, mergedLeaf.reader().maxDoc()); i++) {
                        rowIdDV.advanceExact(i);
                        System.out.printf("%d ", rowIdDV.nextValue());
                    }
                    System.out.println();
                }
            }
        }

        long p2Ms = (System.nanoTime() - p2Start) / 1_000_000;

        // ========== Summary ==========
        System.out.println("\n===== Summary =====");
        System.out.printf("  Parquet merge:      %d ms%n", p1Ms);
        System.out.printf("  Lucene merge:       %d ms (includes addIndexes + merge)%n", p2Ms);
        System.out.printf("  Merged parquet:     %s%n", mergedParquetFile);
        System.out.printf("  Merged lucene:      %s%n", mergedLuceneDir);
        System.out.printf("  RowIdMapping:       %d entries%n", rowIdMapping.size());
        System.out.printf("%nRun CompositeDataVerifier on the merged output to confirm data parity.%n");
    }

    /**
     * Remaps the Parquet RowIdMapping for use with Lucene segments.
     *
     * <p>The Rust merge produces a RowIdMapping keyed by Parquet file IDs
     * (e.g., "0", "1", "2" extracted from filenames). CustomOneMerge.reorder()
     * maps these to Lucene segments via writer_generation attribute, but our
     * harness segments don't have that attribute.
     *
     * <p>Instead, we know the segments are in the same order as the Parquet files
     * (segment index 0 = file "0", etc.), so we remap the RowIdMapping to use
     * segment index as the key, and build a custom OneMerge that maps by
     * segment position rather than writer_generation.
     */
    private static RowIdMapping remapForLuceneSegments(
        RowIdMapping parquetMapping,
        List<SegmentCommitInfo> segments,
        int numSegments,
        int rowsPerSegment
    ) {
        // The parquet mapping already has the right structure — we just need
        // the file IDs to match what our SegmentOrderedOneMerge expects.
        // Since both use "0", "1", "2"... as keys, we can reuse it directly.
        return parquetMapping;
    }

    private static void pushDefaultRustSettings() {
        NativeSettings config = new NativeSettings();
        config.setIndexName(INDEX_NAME);
        config.setCompressionType("zstd");
        config.setCompressionLevel(3);
        config.setPageSizeBytes(1024L * 1024);
        config.setPageRowLimit(20000);
        config.setDictSizeBytes(1024L * 1024);
        config.setRowGroupSizeBytes(128L * 1024 * 1024);
        try {
            RustBridge.onSettingsUpdate(config);
        } catch (Exception e) {
            System.err.println("Warning: Failed to push Rust settings: " + e.getMessage());
        }
    }

    /**
     * A CustomOneMerge variant that maps RowIdMapping file IDs to Lucene segments
     * by segment position order (0, 1, 2...) rather than by writer_generation attribute.
     */
    static class SegmentOrderedOneMerge extends MergePolicy.OneMerge {

        private final RowIdMapping rowIdMapping;
        private int segmentCounter = 0;

        SegmentOrderedOneMerge(List<SegmentCommitInfo> segments, RowIdMapping rowIdMapping) {
            super(segments);
            this.rowIdMapping = rowIdMapping;
        }

        @Override
        public CodecReader wrapForMerge(CodecReader reader) throws IOException {
            int segIdx = segmentCounter++;
            String fileId = String.valueOf(segIdx);
            return new RowIdRemappingCodecReader(reader, rowIdMapping, fileId);
        }

        @Override
        public org.apache.lucene.index.Sorter.DocMap reorder(
            CodecReader reader,
            org.apache.lucene.store.Directory dir,
            java.util.concurrent.Executor executor
        ) throws IOException {
            int totalDocs = reader.maxDoc();

            int[] oldToNew = new int[totalDocs];
            int[] newToOld = new int[totalDocs];

            Map<String, Integer> fileIdToBaseDoc = new HashMap<>();
            int baseDoc = 0;
            for (int i = 0; i < segments.size(); i++) {
                String fileId = String.valueOf(i);
                fileIdToBaseDoc.put(fileId, baseDoc);
                baseDoc += segments.get(i).info.maxDoc();
            }

            for (Map.Entry<String, Integer> entry : rowIdMapping.getFileOffsets().entrySet()) {
                String fileId = entry.getKey();
                Integer segmentBase = fileIdToBaseDoc.get(fileId);
                if (segmentBase == null) continue;

                int fileSize = rowIdMapping.getFileSize(fileId);
                for (int oldRowInFile = 0; oldRowInFile < fileSize; oldRowInFile++) {
                    long newRowId = rowIdMapping.getNewRowId(oldRowInFile, fileId);
                    if (newRowId < 0) continue;

                    int oldDocId = segmentBase + oldRowInFile;
                    int newDocId = (int) newRowId;

                    if (oldDocId < totalDocs && newDocId < totalDocs) {
                        oldToNew[oldDocId] = newDocId;
                        newToOld[newDocId] = oldDocId;
                    }
                }
            }

            return new org.apache.lucene.index.Sorter.DocMap() {
                @Override public int oldToNew(int docID) { return oldToNew[docID]; }
                @Override public int newToOld(int docID) { return newToOld[docID]; }
                @Override public int size() { return totalDocs; }
            };
        }
    }

    /**
     * Wraps a CodecReader to override ___row_id NumericDocValues with new values
     * from the RowIdMapping. For doc i in this segment, the new row_id is
     * rowIdMapping.getNewRowId(i, fileId).
     */
    static class RowIdRemappingCodecReader extends FilterCodecReader {

        private final RowIdMapping rowIdMapping;
        private final String fileId;

        RowIdRemappingCodecReader(CodecReader in, RowIdMapping rowIdMapping, String fileId) {
            super(in);
            this.rowIdMapping = rowIdMapping;
            this.fileId = fileId;
        }

        @Override
        public DocValuesProducer getDocValuesReader() {
            DocValuesProducer delegate = in.getDocValuesReader();
            if (delegate == null) return null;
            return new RowIdRemappingDocValuesProducer(delegate, rowIdMapping, fileId, in.maxDoc());
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return in.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }

    /**
     * DocValuesProducer that intercepts ___row_id and returns remapped values.
     */
    static class RowIdRemappingDocValuesProducer extends DocValuesProducer {

        private final DocValuesProducer delegate;
        private final RowIdMapping rowIdMapping;
        private final String fileId;
        private final int maxDoc;

        RowIdRemappingDocValuesProducer(
            DocValuesProducer delegate, RowIdMapping rowIdMapping, String fileId, int maxDoc
        ) {
            this.delegate = delegate;
            this.rowIdMapping = rowIdMapping;
            this.fileId = fileId;
            this.maxDoc = maxDoc;
        }

        @Override
        public NumericDocValues getNumeric(FieldInfo field) throws IOException {
            return delegate.getNumeric(field);
        }

        @Override
        public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
            if (ROW_ID.equals(field.name)) {
                return new SortedNumericDocValues() {
                    private int docID = -1;

                    @Override
                    public long nextValue() {
                        return rowIdMapping.getNewRowId(docID, fileId);
                    }

                    @Override
                    public int docValueCount() { return 1; }

                    @Override
                    public boolean advanceExact(int target) {
                        docID = target;
                        return true;
                    }

                    @Override
                    public int docID() { return docID; }

                    @Override
                    public int nextDoc() { return ++docID < maxDoc ? docID : NO_MORE_DOCS; }

                    @Override
                    public int advance(int target) {
                        docID = target;
                        return docID < maxDoc ? docID : NO_MORE_DOCS;
                    }

                    @Override
                    public long cost() { return maxDoc; }
                };
            }
            return delegate.getSortedNumeric(field);
        }

        @Override
        public BinaryDocValues getBinary(FieldInfo field) throws IOException {
            return delegate.getBinary(field);
        }

        @Override
        public SortedDocValues getSorted(FieldInfo field) throws IOException {
            return delegate.getSorted(field);
        }

        @Override
        public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
            return delegate.getSortedSet(field);
        }

        @Override
        public DocValuesSkipper getSkipper(FieldInfo field) throws IOException {
            return delegate.getSkipper(field);
        }

        @Override
        public void checkIntegrity() throws IOException {
            delegate.checkIntegrity();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
