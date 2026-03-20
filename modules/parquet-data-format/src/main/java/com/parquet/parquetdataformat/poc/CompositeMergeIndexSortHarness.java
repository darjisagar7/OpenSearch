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
import org.apache.lucene.index.IndexSorter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.index.engine.exec.merge.RowIdMapping;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

/**
 * Composite merge harness using Lucene's normal IndexSort (not reorder).
 *
 * <p>Uses a custom {@link SortedNumericSortField} whose {@code getIndexSorter()}
 * returns remapped ___row_id values via the {@link RowIdMapping}. The source
 * segments are added via {@code addIndexes(CodecReader...)} which triggers
 * Lucene's normal index-sort merge, reordering docs by remapped row IDs.
 */
public class CompositeMergeIndexSortHarness {

    private static final String ROW_ID = "___row_id";
    private static final String INDEX_NAME = "composite_bench";

    public static void main(String[] args) throws Exception {
        Path dataDir = args.length > 0
            ? Path.of(args[0])
            : Path.of("/Users/darsaga/lucene-index");

        Path luceneDir = dataDir.resolve("sorted_lucene");
        Path parquetDir = dataDir.resolve("sorted_parquet");
        Path metaFile = dataDir.resolve("ingest_meta.properties");

        Properties props = new Properties();
        props.load(Files.newBufferedReader(metaFile));
        int numSegments = Integer.parseInt(props.getProperty("numSegments"));
        int rowsPerSegment = Integer.parseInt(props.getProperty("rowsPerSegment"));
        long totalRows = Long.parseLong(props.getProperty("totalRows"));

        System.out.printf("Input: %d segments, %d rows/segment, %d total rows%n",
            numSegments, rowsPerSegment, totalRows);

        pushDefaultRustSettings();

        List<Path> parquetFiles;
        try (Stream<Path> s = Files.list(parquetDir)) {
            parquetFiles = s.filter(p -> p.toString().endsWith(".parquet"))
                .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                .toList();
        }
        System.out.printf("Parquet files: %d%n", parquetFiles.size());

        // ========== Phase 1: Parquet merge ==========
        System.out.println("\n===== Phase 1: Parquet merge (primary — Rust) =====");

        Path mergedParquetDir = dataDir.resolve("sorted_parquet_merged");
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

        System.out.println("  File offsets:");
        rowIdMapping.getFileOffsets().forEach((fileId, offset) ->
            System.out.printf("    fileId=%s, offset=%d, size=%d%n",
                fileId, offset, rowIdMapping.getFileSize(fileId)));

        // ========== Phase 2: Lucene merge (IndexSort) ==========
        System.out.println("\n===== Phase 2: Lucene merge (secondary — IndexSort) =====");

        Path mergedLuceneDir = dataDir.resolve("sorted_lucene_merged");
        Files.createDirectories(mergedLuceneDir);

        long p2Start = System.nanoTime();

        // Maps reader identity → parquet fileId. Populated before addIndexes.
        IdentityHashMap<LeafReader, String> readerToFileId = new IdentityHashMap<>();

        try (Directory srcDir = NIOFSDirectory.open(luceneDir);
             DirectoryReader srcReader = DirectoryReader.open(srcDir);
             Directory dstDir = NIOFSDirectory.open(mergedLuceneDir)) {

            // Build CodecReader array and register fileId for each leaf BEFORE addIndexes
            List<LeafReaderContext> leaves = srcReader.leaves();
            CodecReader[] codecReaders = new CodecReader[leaves.size()];
            System.out.printf("  Source segments: %d%n", leaves.size());
            // Wrap each CodecReader to remap ___row_id doc values
            for (int i = 0; i < leaves.size(); i++) {
                LeafReader leaf = leaves.get(i).reader();
                String fileId = String.valueOf(i);
                readerToFileId.put(leaf, fileId);
                codecReaders[i] = new RowIdRemappingCodecReader(
                    (CodecReader) leaf, rowIdMapping, fileId);
                // Also register the wrapped reader so resolveFileId can find it
                readerToFileId.put(codecReaders[i], fileId);
                System.out.printf("    segment %d: maxDoc=%d, fileId=%s%n",
                    i, leaf.maxDoc(), fileId);
            }

            // Custom sort field that remaps ___row_id through RowIdMapping
            RowIdMappingSortField sortField = new RowIdMappingSortField(
                ROW_ID, rowIdMapping, readerToFileId);

            IndexWriterConfig iwc = new IndexWriterConfig();
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            iwc.setMergeScheduler(new SerialMergeScheduler());
            iwc.setIndexSort(new Sort(sortField));

            try (MergeableIndexSortWriter writer = new MergeableIndexSortWriter(dstDir, iwc)) {
                // addIndexes(CodecReader...) merges all readers into one segment,
                // applying the index sort (our custom RowIdMapping sort).
                long mergeStart = System.nanoTime();
                writer.addIndexes(codecReaders);
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
                    System.out.printf("  Total docs: %d%n", reader.numDocs());

                    var mergedLeaf = reader.leaves().stream()
                        .max(Comparator.comparingInt(l -> l.reader().maxDoc()))
                        .orElseThrow();
                    System.out.printf("  Merged segment docs: %d%n", mergedLeaf.reader().maxDoc());

                    SortedNumericDocValues rowIdDV = mergedLeaf.reader()
                        .getSortedNumericDocValues(ROW_ID);
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

        System.out.println("\n===== Summary =====");
        System.out.printf("  Parquet merge:      %d ms%n", p1Ms);
        System.out.printf("  Lucene merge:       %d ms%n", p2Ms);
        System.out.printf("  Merged parquet:     %s%n", mergedParquetFile);
        System.out.printf("  Merged lucene:      %s%n", mergedLuceneDir);
        System.out.printf("  RowIdMapping:       %d entries%n", rowIdMapping.size());
        System.out.printf("%nRun CompositeDataVerifier on the merged output to confirm data parity.%n");
    }

    /**
     * Custom SortedNumericSortField that overrides getIndexSorter() to apply
     * RowIdMapping during Lucene's normal index sort.
     *
     * <p>Uses an IdentityHashMap to map each LeafReader instance to its fileId,
     * so the remapped NumericDocValues knows which RowIdMapping file to use.
     */
    static class RowIdMappingSortField extends SortedNumericSortField {

        private final RowIdMapping rowIdMapping;
        private final IdentityHashMap<LeafReader, String> readerToFileId;

        RowIdMappingSortField(String field, RowIdMapping rowIdMapping,
                              IdentityHashMap<LeafReader, String> readerToFileId) {
            super(field, SortField.Type.LONG);
            this.rowIdMapping = rowIdMapping;
            this.readerToFileId = readerToFileId;
        }

        /** No-remap constructor for ingestion — behaves like plain SortedNumericSortField. */
        RowIdMappingSortField(String field) {
            super(field, SortField.Type.LONG);
            this.rowIdMapping = null;
            this.readerToFileId = null;
        }

        @Override
        public IndexSorter getIndexSorter() {
            if (rowIdMapping == null) {
                return super.getIndexSorter();
            }
            return new IndexSorter.LongSorter(
                SortedNumericSortField.Provider.NAME,
                (Long) getMissingValue(),
                getReverse(),
                reader -> new RemappedNumericDocValues(
                    reader, ROW_ID, rowIdMapping, readerToFileId)
            );
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (!(obj instanceof SortedNumericSortField)) return false;
            SortedNumericSortField other = (SortedNumericSortField) obj;
            return getField().equals(other.getField())
                && getType() == other.getType()
                && getReverse() == other.getReverse()
                && java.util.Objects.equals(getMissingValue(), other.getMissingValue())
                && getSelector() == other.getSelector()
                && getNumericType() == other.getNumericType();
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }
    }

    /**
     * NumericDocValues that reads original ___row_id from SortedNumericDocValues
     * and returns the remapped value from RowIdMapping.
     */
    static class RemappedNumericDocValues extends NumericDocValues {

        private final SortedNumericDocValues delegate;
        private final RowIdMapping rowIdMapping;
        private final String fileId;

        RemappedNumericDocValues(LeafReader reader, String field,
                                 RowIdMapping rowIdMapping,
                                 IdentityHashMap<LeafReader, String> readerToFileId) throws IOException {
            this.delegate = reader.getSortedNumericDocValues(field);
            this.rowIdMapping = rowIdMapping;
            this.fileId = resolveFileId(reader, readerToFileId);
        }

        private static String resolveFileId(LeafReader reader,
                                             IdentityHashMap<LeafReader, String> readerToFileId) {
            // Direct identity lookup
            String fileId = readerToFileId.get(reader);
            if (fileId != null) return fileId;

            // Unwrap FilterLeafReader layers and try again
            LeafReader unwrapped = reader;
            while (unwrapped instanceof org.apache.lucene.index.FilterLeafReader) {
                unwrapped = ((org.apache.lucene.index.FilterLeafReader) unwrapped).getDelegate();
                fileId = readerToFileId.get(unwrapped);
                if (fileId != null) return fileId;
            }

            // Fallback: writer_generation attribute (production path)
            if (reader.getFieldInfos().fieldInfo(ROW_ID) != null) {
                String writerGen = reader.getFieldInfos()
                    .fieldInfo(ROW_ID).getAttribute("writer_gen");
                if (writerGen != null) return writerGen;
            }

            throw new IllegalStateException("Cannot resolve fileId for reader: " + reader
                + " (class=" + reader.getClass().getName() + ")");
        }

        @Override
        public long longValue() throws IOException {
            long originalRowId = delegate.nextValue();
            long remapped = rowIdMapping.getNewRowId(originalRowId, fileId);
            return remapped >= 0 ? remapped : originalRowId;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return delegate.advanceExact(target);
        }

        @Override
        public int docID() {
            return delegate.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            return delegate.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            return delegate.advance(target);
        }

        @Override
        public long cost() {
            return delegate.cost();
        }
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
     * Wraps a CodecReader to replace ___row_id SortedNumericDocValues with
     * remapped global values from the RowIdMapping.
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
     * DocValuesProducer that intercepts ___row_id and returns remapped global values.
     * All other fields are delegated unchanged.
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
