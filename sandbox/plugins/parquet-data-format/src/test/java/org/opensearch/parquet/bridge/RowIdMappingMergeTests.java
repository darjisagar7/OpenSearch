/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.index.engine.dataformat.PackedRowIdMapping;
import org.opensearch.index.engine.dataformat.RowIdMapping;
import org.opensearch.nativebridge.spi.ArrowExport;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Validates {@link RowIdMapping} correctness by reading both original and merged
 * Parquet files back from disk and cross-referencing every mapping entry.
 *
 * <p>For each {@code (generation, oldRowId) → newRowId} in the mapping the test
 * reads the original file's row at position {@code oldRowId} and the merged
 * file's row whose {@code __row_id__ == newRowId}, then asserts that every data
 * column is identical. This is a ground-truth check — both sides come from
 * actual Parquet files on disk, not from in-memory Java arrays.
 */
public class RowIdMappingMergeTests extends OpenSearchTestCase {

    private static final String INDEX = "rowid-mapping-test";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<List<Map<String, Object>>> LIST_OF_MAPS = new TypeReference<>() {
    };

    private BufferAllocator allocator;
    private Schema arrowSchema;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        RustBridge.initLogger();
        allocator = new RootAllocator();
        arrowSchema = new Schema(
            List.of(
                new Field("timestamp", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("tag", FieldType.nullable(new ArrowType.Utf8()), null)
            )
        );
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    // ────────────────────────────────────────────────────────────────────
    // Test: 10 segments, sorted merge, heavily interleaved & tricky data
    // ────────────────────────────────────────────────────────────────────

    public void testSortedMergeTenSegments() throws Exception {
        pushSettings();
        Path dir = createTempDir();

        // Deliberately adversarial data:
        // ascending, descending-within-file, single-row, all-dups,
        // large gap, interleaved, negatives, large segment with dups,
        // reverse-large with dups, another single-row
        long[][] timestamps = {
            { 10, 20, 30 },                              // 0: ascending
            { 90, 60, 30 },                              // 1: descending, dup 30
            { 50 },                                      // 2: single row
            { 40, 40, 40, 40 },                          // 3: all duplicates
            { 1, 1000 },                                 // 4: large gap
            { 15, 25, 35 },                              // 5: interleaved with 0
            { -100, -50, 0 },                            // 6: negatives
            { 5, 10, 15, 20, 25, 30, 35, 40, 45, 50 },  // 7: large, many dups
            { 999, 500, 100, 50, 10 },                   // 8: reverse, dups
            { 42 },                                      // 9: single row
        };

        Fixture f = writeOriginalsMergeAndRead(dir, timestamps, "sorted.parquet");

        // Merged output must be globally sorted ascending on timestamp
        for (int i = 1; i < f.mergedRows.size(); i++) {
            assertTrue("Not sorted at position " + i, num(f.mergedRows.get(i - 1), "timestamp") <= num(f.mergedRows.get(i), "timestamp"));
        }

        // __row_id__ must be sequential 0..N-1
        for (int i = 0; i < f.mergedRows.size(); i++) {
            assertEquals((long) i, num(f.mergedRows.get(i), "__row_id__"));
        }

        assertMappingMatchesFiles(f);
        removeSettings();
    }

    // ────────────────────────────────────────────────────────────────────
    // Test: 10 segments, unsorted merge
    // ────────────────────────────────────────────────────────────────────

    public void testUnsortedMergeTenSegments() throws Exception {
        // Push settings without sort columns → unsorted merge path
        RustBridge.removeSettings(INDEX);
        RustBridge.onSettingsUpdate(NativeSettings.builder().indexName(INDEX).compressionType("LZ4_RAW").build());

        Path dir = createTempDir();

        long[][] timestamps = {
            { 300, 100, 200 },
            { 600, 400, 500 },
            { 900, 700, 800 },
            { 50 },
            { 1000, 2000, 3000, 4000 },
            { 1 },
            { 99, 98, 97, 96, 95 },
            { 10, 20 },
            { 500, 500, 500 },
            { 42, 43 }, };

        Fixture f = writeOriginalsMergeAndRead(dir, timestamps, "unsorted.parquet");

        // __row_id__ must be sequential
        for (int i = 0; i < f.mergedRows.size(); i++) {
            assertEquals((long) i, num(f.mergedRows.get(i), "__row_id__"));
        }

        assertMappingMatchesFiles(f);
        removeSettings();
    }

    // ────────────────────────────────────────────────────────────────────
    // Test: 10 segments, perfect interleave → merged output is 1..50
    // ────────────────────────────────────────────────────────────────────

    public void testInterleavedSortMergeTenSegments() throws Exception {
        pushSettings();
        Path dir = createTempDir();

        // file i has timestamps {i+1, i+11, i+21, i+31, i+41}
        long[][] timestamps = new long[10][5];
        for (int f = 0; f < 10; f++) {
            for (int r = 0; r < 5; r++) {
                timestamps[f][r] = (f + 1) + (r * 10L);
            }
        }

        Fixture f = writeOriginalsMergeAndRead(dir, timestamps, "interleaved.parquet");

        // Merged output must be exactly 1, 2, 3, …, 50
        for (int i = 0; i < f.mergedRows.size(); i++) {
            assertEquals((long) (i + 1), num(f.mergedRows.get(i), "timestamp"));
        }

        assertMappingMatchesFiles(f);
        removeSettings();
    }

    // ────────────────────────────────────────────────────────────────────
    // Test: Non-sequential writer generations (sorted merge)
    // ────────────────────────────────────────────────────────────────────

    public void testSortedMergeNonSequentialGenerations() throws Exception {
        pushSettings();
        Path dir = createTempDir();

        long[][] timestamps = { { 10, 30, 50 }, { 20, 40, 60 }, { 5, 25, 45 }, };
        // Realistic non-sequential generations as would occur in production
        long[] generations = { 7, 3, 12 };

        Fixture f = writeOriginalsMergeAndRead(dir, timestamps, "nonsec_sorted.parquet", generations);

        // Verify sorted output
        for (int i = 1; i < f.mergedRows.size(); i++) {
            assertTrue(num(f.mergedRows.get(i - 1), "timestamp") <= num(f.mergedRows.get(i), "timestamp"));
        }

        assertMappingMatchesFiles(f);
        removeSettings();
    }

    // ────────────────────────────────────────────────────────────────────
    // Test: Non-sequential writer generations (unsorted merge)
    // ────────────────────────────────────────────────────────────────────

    public void testUnsortedMergeNonSequentialGenerations() throws Exception {
        RustBridge.removeSettings(INDEX);
        RustBridge.onSettingsUpdate(NativeSettings.builder().indexName(INDEX).compressionType("LZ4_RAW").build());
        Path dir = createTempDir();

        long[][] timestamps = { { 100, 200, 300 }, { 400, 500 }, { 600, 700, 800, 900 }, };
        long[] generations = { 42, 7, 100 };

        Fixture f = writeOriginalsMergeAndRead(dir, timestamps, "nonsec_unsorted.parquet", generations);
        assertMappingMatchesFiles(f);
        removeSettings();
    }

    // ────────────────────────────────────────────────────────────────────
    // Test: Large generation values
    // ────────────────────────────────────────────────────────────────────

    public void testSortedMergeLargeGenerationValues() throws Exception {
        pushSettings();
        Path dir = createTempDir();

        long[][] timestamps = { { 1, 3, 5 }, { 2, 4, 6 }, };
        long[] generations = { 1_000_000_000L, Long.MAX_VALUE / 2 };

        Fixture f = writeOriginalsMergeAndRead(dir, timestamps, "large_gen.parquet", generations);

        for (int i = 1; i < f.mergedRows.size(); i++) {
            assertTrue(num(f.mergedRows.get(i - 1), "timestamp") <= num(f.mergedRows.get(i), "timestamp"));
        }

        assertMappingMatchesFiles(f);
        removeSettings();
    }

    // ────────────────────────────────────────────────────────────────────
    // Test: Single file merge
    // ────────────────────────────────────────────────────────────────────

    public void testSortedMergeSingleFile() throws Exception {
        pushSettings();
        Path dir = createTempDir();

        long[][] timestamps = { { 30, 10, 20 } };
        long[] generations = { 55 };

        Fixture f = writeOriginalsMergeAndRead(dir, timestamps, "single.parquet", generations);

        // Single file sorted: output should be 10, 20, 30
        assertEquals(10L, num(f.mergedRows.get(0), "timestamp"));
        assertEquals(20L, num(f.mergedRows.get(1), "timestamp"));
        assertEquals(30L, num(f.mergedRows.get(2), "timestamp"));

        assertMappingMatchesFiles(f);
        removeSettings();
    }

    // ────────────────────────────────────────────────────────────────────
    // Test: Reverse-order generations (descending gen values)
    // ────────────────────────────────────────────────────────────────────

    public void testSortedMergeReverseOrderGenerations() throws Exception {
        pushSettings();
        Path dir = createTempDir();

        long[][] timestamps = { { 50, 60 }, { 10, 20 }, { 30, 40 }, { 70, 80 }, };
        // Generations in descending order — not correlated with file content
        long[] generations = { 999, 500, 200, 1 };

        Fixture f = writeOriginalsMergeAndRead(dir, timestamps, "reverse_gen.parquet", generations);

        for (int i = 1; i < f.mergedRows.size(); i++) {
            assertTrue(num(f.mergedRows.get(i - 1), "timestamp") <= num(f.mergedRows.get(i), "timestamp"));
        }

        assertMappingMatchesFiles(f);
        removeSettings();
    }

    // ────────────────────────────────────────────────────────────────────
    // Test: Gaps in generation sequence with varying file sizes
    // ────────────────────────────────────────────────────────────────────

    public void testUnsortedMergeGappyGenerationsVaryingSizes() throws Exception {
        RustBridge.removeSettings(INDEX);
        RustBridge.onSettingsUpdate(NativeSettings.builder().indexName(INDEX).compressionType("LZ4_RAW").build());
        Path dir = createTempDir();

        long[][] timestamps = {
            { 1 },                                       // 1 row
            { 10, 20, 30, 40, 50, 60, 70, 80, 90, 100 },// 10 rows
            { 200, 300 },                                // 2 rows
            { 400 },                                     // 1 row
            { 500, 600, 700 },                           // 3 rows
        };
        // Large gaps between generations
        long[] generations = { 5, 100, 250, 10_000, 99_999 };

        Fixture f = writeOriginalsMergeAndRead(dir, timestamps, "gappy_unsorted.parquet", generations);
        assertMappingMatchesFiles(f);
        removeSettings();
    }

    /**
     * For every {@code (generation, oldRowId)} in the mapping:
     * <ol>
     *   <li>Read the original file's row at {@code oldRowId} (from disk).</li>
     *   <li>Translate via the mapping: {@code newRowId = mapping.getNewRowId(oldRowId, generation)}.</li>
     *   <li>Read the merged file's row whose {@code __row_id__ == newRowId} (from disk).</li>
     *   <li>Assert every data column matches.</li>
     * </ol>
     * Also checks bijectivity (every merged position covered exactly once)
     * and boundary conditions (invalid lookups return -1).
     */
    private void assertMappingMatchesFiles(Fixture f) {
        RowIdMapping mapping = f.mapping;
        assertNotNull("mapping must not be null", mapping);
        assertTrue("expected PackedRowIdMapping", mapping instanceof PackedRowIdMapping);
        PackedRowIdMapping packed = (PackedRowIdMapping) mapping;

        int totalRows = f.mergedRows.size();
        assertEquals("mapping size must equal merged row count", totalRows, packed.size());
        assertEquals("generation count must equal file count", f.originals.size(), packed.getGenerationOffsets().size());

        // Index merged rows by __row_id__ for O(1) lookup
        Map<Long, Map<String, Object>> mergedById = new HashMap<>(totalRows);
        for (Map<String, Object> row : f.mergedRows) {
            mergedById.put(num(row, "__row_id__"), row);
        }

        boolean[] seen = new boolean[totalRows];

        for (int fileIdx = 0; fileIdx < f.originals.size(); fileIdx++) {
            long gen = f.generations[fileIdx];
            List<Map<String, Object>> origRows = f.originals.get(fileIdx);
            assertEquals("gen " + gen + " size", origRows.size(), packed.getGenerationSize(gen));

            for (int old = 0; old < origRows.size(); old++) {
                long newId = packed.getNewRowId(old, gen);
                assertTrue("newId negative: gen=" + gen + " old=" + old, newId >= 0);
                assertTrue("newId out of range: " + newId, newId < totalRows);

                Map<String, Object> orig = origRows.get(old);
                Map<String, Object> merged = mergedById.get(newId);
                assertNotNull("no merged row at __row_id__=" + newId, merged);

                // Cross-check every data column
                assertEquals("timestamp: gen=" + gen + " old=" + old + " new=" + newId, num(orig, "timestamp"), num(merged, "timestamp"));
                assertEquals("value: gen=" + gen + " old=" + old + " new=" + newId, num(orig, "value"), num(merged, "value"));
                assertEquals("tag: gen=" + gen + " old=" + old + " new=" + newId, orig.get("tag"), merged.get("tag"));

                assertFalse("position " + newId + " mapped twice", seen[(int) newId]);
                seen[(int) newId] = true;
            }
        }

        // Every merged position must be covered
        for (int i = 0; i < totalRows; i++) {
            assertTrue("position " + i + " not covered", seen[i]);
        }

        // Boundary: invalid lookups return -1
        assertEquals(-1L, packed.getNewRowId(0, 999_999));
        assertEquals(-1L, packed.getNewRowId(-1, 0));
        assertEquals(-1L, packed.getNewRowId(999_999, 0));
    }

    // ════════════════════════════════════════════════════════════════════
    // Harness: write originals, read them back, merge, read merged back
    // ════════════════════════════════════════════════════════════════════

    private record Fixture(List<List<Map<String, Object>>> originals, List<Map<String, Object>> mergedRows, RowIdMapping mapping,
        long[] generations) {
    }

    private Fixture writeOriginalsMergeAndRead(Path dir, long[][] fileTimestamps, String mergedName) throws Exception {
        long[] generations = new long[fileTimestamps.length];
        for (int i = 0; i < generations.length; i++)
            generations[i] = i;
        return writeOriginalsMergeAndRead(dir, fileTimestamps, mergedName, generations);
    }

    private Fixture writeOriginalsMergeAndRead(Path dir, long[][] fileTimestamps, String mergedName, long[] generations) throws Exception {
        List<String> paths = new ArrayList<>();
        List<List<Map<String, Object>>> originals = new ArrayList<>();
        int total = 0;

        for (int fi = 0; fi < fileTimestamps.length; fi++) {
            long[] ts = fileTimestamps[fi];
            int[] vals = new int[ts.length];
            String[] tags = new String[ts.length];
            for (int r = 0; r < ts.length; r++) {
                vals[r] = fi * 1000 + r;
                tags[r] = "g" + fi + "r" + r;
            }

            String path = writeParquetFile(dir, "seg" + fi + ".parquet", ts, vals, tags, generations[fi]);
            paths.add(path);
            total += ts.length;

            // Read the original file back from disk — ground truth
            List<Map<String, Object>> rows = JSON.readValue(RustBridge.readAsJson(path), LIST_OF_MAPS);
            assertEquals(ts.length, rows.size());
            originals.add(rows);
        }

        // Merge
        String merged = dir.resolve(mergedName).toString();
        RowIdMapping mapping = RustBridge.mergeParquetFilesInRust(paths.stream().map(Path::of).toList(), merged, INDEX);

        assertEquals(total, RustBridge.getFileMetadata(merged).numRows());

        // Read merged file back from disk — what we validate against
        List<Map<String, Object>> mergedRows = JSON.readValue(RustBridge.readAsJson(merged), LIST_OF_MAPS);
        assertEquals(total, mergedRows.size());

        return new Fixture(originals, mergedRows, mapping, generations);
    }

    // ════════════════════════════════════════════════════════════════════
    // Low-level helpers
    // ════════════════════════════════════════════════════════════════════

    private void pushSettings() throws Exception {
        RustBridge.onSettingsUpdate(NativeSettings.builder().indexName(INDEX).compressionType("LZ4_RAW").build());
    }

    private void removeSettings() {
        RustBridge.removeSettings(INDEX);
    }

    private String writeParquetFile(Path dir, String name, long[] ts, int[] vals, String[] tags, long generation) throws Exception {
        String path = dir.resolve(name).toString();
        ParquetSortConfig sort = new ParquetSortConfig(List.of("timestamp"), List.of(false), List.of(false));

        try (ArrowExport schemaExp = exportSchema()) {
            NativeParquetWriter w = new NativeParquetWriter(path, INDEX, schemaExp.getSchemaAddress(), sort, generation);
            try (ArrowExport dataExp = exportData(ts, vals, tags)) {
                w.write(dataExp.getArrayAddress(), dataExp.getSchemaAddress());
            }
            w.flush();
        }
        return path;
    }

    private ArrowExport exportSchema() {
        ArrowSchema s = ArrowSchema.allocateNew(allocator);
        Data.exportSchema(allocator, arrowSchema, null, s);
        return new ArrowExport(null, s);
    }

    private ArrowExport exportData(long[] ts, int[] vals, String[] tags) {
        try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
            BigIntVector tsVec = (BigIntVector) root.getVector("timestamp");
            IntVector valVec = (IntVector) root.getVector("value");
            VarCharVector tagVec = (VarCharVector) root.getVector("tag");
            for (int i = 0; i < ts.length; i++) {
                tsVec.setSafe(i, ts[i]);
                valVec.setSafe(i, vals[i]);
                tagVec.setSafe(i, tags[i].getBytes(StandardCharsets.UTF_8));
            }
            root.setRowCount(ts.length);

            ArrowArray arr = ArrowArray.allocateNew(allocator);
            ArrowSchema sch = ArrowSchema.allocateNew(allocator);
            Data.exportVectorSchemaRoot(allocator, root, null, arr, sch);
            return new ArrowExport(arr, sch);
        }
    }

    private static long num(Map<String, Object> row, String col) {
        return ((Number) row.get(col)).longValue();
    }
}
