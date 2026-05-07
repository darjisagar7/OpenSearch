/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergeIndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.be.lucene.merge.LuceneMerger;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.MergeInput;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.dataformat.RowIdMapping;
import org.opensearch.index.engine.exec.Segment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Standalone demo that creates 5 Lucene segments, merges them using {@link LuceneMerger},
 * and prints the result. Run this, then run {@link LuceneMergedIndexReader} to read the output.
 *
 * <p>Usage: Run as a Java main class. The index is written to {@code /tmp/lucene-merger-demo}.
 */
@SuppressForbidden(reason = "Demo CLI tool uses System.out and reflection to access IndexWriter internals")
public class LuceneMergerDemo {

    private static final String ROW_ID_FIELD = DocumentInput.ROW_ID_FIELD;
    private static final String WRITER_GENERATION_ATTR = "writer_generation";
    private static final Path INDEX_DIR = Path.of("/tmp/lucene-merger-demo");

    public static void main(String[] args) throws Exception {
        // Clean up any previous run
        if (Files.exists(INDEX_DIR)) {
            Files.walk(INDEX_DIR).sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    // ignore
                }
            });
        }
        Files.createDirectories(INDEX_DIR);

        System.out.println("=== LuceneMerger Demo ===");
        System.out.println("Index directory: " + INDEX_DIR);
        System.out.println();

        // Create an IndexWriter with NoMergePolicy so Lucene does not auto-merge our segments.
        // SortedNumericSortField on the row ID field enables IndexSort-based reordering during merge.
        IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
        iwc.setMergeScheduler(new SerialMergeScheduler());
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        iwc.setIndexSort(new Sort(new SortedNumericSortField(ROW_ID_FIELD, SortField.Type.LONG)));

        try (Directory directory = NIOFSDirectory.open(INDEX_DIR); MergeIndexWriter writer = new MergeIndexWriter(directory, iwc)) {

            // --- Step 1: Write 5 segments, each with a different writer_generation ---
            // Each segment's ___row_id starts from 0 (local to the segment).
            // Segment layout:
            // gen=1: doc_0, doc_1, doc_2 (3 docs, row IDs 0-2)
            // gen=2: doc_3, doc_4 (2 docs, row IDs 0-1)
            // gen=3: doc_5, doc_6, doc_7 (3 docs, row IDs 0-2)
            // gen=4: doc_8 (1 doc, row ID 0)
            // gen=5: doc_9, doc_10, doc_11, doc_12 (4 docs, row IDs 0-3)
            int[][] segmentSpecs = {
                // { generation, globalDocIdStart, numDocs }
                { 1, 0, 3 },
                { 2, 3, 2 },
                { 3, 5, 3 },
                { 4, 8, 1 },
                { 5, 9, 4 }, };

            for (int[] spec : segmentSpecs) {
                writeSegment(writer, spec[0], spec[1], spec[2]);
                System.out.printf("  Written segment gen=%d: %d docs (___row_id__ 0-%d)%n", spec[0], spec[2], spec[2] - 1);
            }
            writer.commit();

            SegmentInfos infos = getSegmentInfos(writer);
            System.out.println();
            System.out.println("Total segments before merge: " + infos.size());
            System.out.println("Total docs before merge:     " + writer.getDocStats().numDocs);
            System.out.println();

            // --- Step 2: Build a RowIdMapping that maps local per-segment row IDs to new global row IDs ---
            // This simulates the primary data format producing a new global ordering (0..12).
            // The old row IDs are local to each segment (starting from 0).
            // Within each generation, the remapped values are ascending (preserving within-segment order).
            //
            // gen=1 (3 rows): local 0→0, 1→3, 2→6 (interleaved with other gens)
            // gen=2 (2 rows): local 0→1, 1→4 (interleaved)
            // gen=3 (3 rows): local 0→2, 1→5, 2→8 (interleaved)
            // gen=4 (1 row): local 0→7 (single row)
            // gen=5 (4 rows): local 0→9, 1→10, 2→11, 3→12 (contiguous block at end)
            Map<Long, Map<Long, Long>> mappingTable = new HashMap<>();
            mappingTable.put(1L, Map.of(0L, 0L, 1L, 3L, 2L, 6L));
            mappingTable.put(2L, Map.of(0L, 1L, 1L, 4L));
            mappingTable.put(3L, Map.of(0L, 2L, 1L, 5L, 2L, 8L));
            mappingTable.put(4L, Map.of(0L, 7L));
            mappingTable.put(5L, Map.of(0L, 9L, 1L, 10L, 2L, 11L, 3L, 12L));

            RowIdMapping rowIdMapping = (oldId, oldGeneration) -> {
                Map<Long, Long> genMap = mappingTable.get(oldGeneration);
                if (genMap != null && genMap.containsKey(oldId)) {
                    return genMap.get(oldId);
                }
                return oldId; // identity fallback
            };

            // --- Step 3: Build MergeInput from the 5 segments ---
            List<Segment> segments = new ArrayList<>();
            for (SegmentCommitInfo sci : infos.asList()) {
                String genAttr = sci.info.getAttribute(WRITER_GENERATION_ATTR);
                if (genAttr != null) {
                    segments.add(Segment.builder(Long.parseLong(genAttr)).build());
                }
            }

            MergeInput mergeInput = MergeInput.builder().segments(segments).rowIdMapping(rowIdMapping).newWriterGeneration(100L).build();

            // --- Step 4: Run the merge ---
            System.out.println("Running LuceneMerger.merge() with RowIdMapping...");
            LuceneMerger merger = new LuceneMerger(writer, new LuceneDataFormat(), INDEX_DIR);
            MergeResult result = merger.merge(mergeInput);

            System.out.println("Merge complete.");
            System.out.println("  RowIdMapping present in result: " + result.rowIdMapping().isPresent());
            System.out.println();

            // Commit so the merged segment is visible to readers
            writer.commit();

            SegmentInfos infosAfter = getSegmentInfos(writer);
            System.out.println("Total segments after merge: " + infosAfter.size());
            System.out.println("Total docs after merge:     " + writer.getDocStats().numDocs);
            System.out.println();
            System.out.println("Index written to: " + INDEX_DIR);
            System.out.println("Run LuceneMergedIndexReader to inspect the merged index.");
        }
    }

    /**
     * Writes a segment with the given generation and number of docs.
     * Each segment's ___row_id starts from 0 (local to the segment).
     * The globalDocIdStart is used only for the "id" and "data" stored fields to keep doc names unique.
     */
    private static void writeSegment(IndexWriter writer, long generation, int globalDocIdStart, int numDocs) throws IOException {
        for (int i = 0; i < numDocs; i++) {
            int globalDocId = globalDocIdStart + i;
            Document doc = new Document();
            doc.add(new StringField("id", "doc_" + globalDocId, Field.Store.YES));
            doc.add(new StoredField("data", "payload_for_doc_" + globalDocId));
            doc.add(new StoredField("category", "gen_" + generation));
            // ___row_id is local to the segment: 0, 1, 2, ...
            doc.add(new SortedNumericDocValuesField(ROW_ID_FIELD, i));
            writer.addDocument(doc);
        }
        writer.flush();
        setWriterGenerationOnLatestSegment(writer, generation);
    }

    /**
     * Uses reflection to stamp the writer_generation attribute on the most recently flushed segment.
     */
    private static void setWriterGenerationOnLatestSegment(IndexWriter writer, long generation) throws IOException {
        try {
            java.lang.reflect.Field segInfosField = IndexWriter.class.getDeclaredField("segmentInfos");
            segInfosField.setAccessible(true);
            SegmentInfos segInfos = (SegmentInfos) segInfosField.get(writer);
            if (segInfos.size() > 0) {
                SegmentCommitInfo lastSegment = segInfos.asList().get(segInfos.size() - 1);
                if (lastSegment.info.getAttribute(WRITER_GENERATION_ATTR) == null) {
                    lastSegment.info.putAttribute(WRITER_GENERATION_ATTR, String.valueOf(generation));
                }
            }
        } catch (ReflectiveOperationException e) {
            throw new IOException("Failed to set writer_generation via reflection", e);
        }
    }

    /**
     * Uses reflection to access the IndexWriter's SegmentInfos.
     */
    private static SegmentInfos getSegmentInfos(IndexWriter writer) throws IOException {
        try {
            java.lang.reflect.Field segInfosField = IndexWriter.class.getDeclaredField("segmentInfos");
            segInfosField.setAccessible(true);
            return (SegmentInfos) segInfosField.get(writer);
        } catch (ReflectiveOperationException e) {
            throw new IOException("Failed to access segmentInfos via reflection", e);
        }
    }
}
