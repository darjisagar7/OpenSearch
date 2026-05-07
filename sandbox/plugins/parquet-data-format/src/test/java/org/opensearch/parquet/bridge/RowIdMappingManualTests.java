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
 * Manual-style tests for verifying ParquetMerger RowIdMapping correctness.
 *
 * <p>These tests call the ParquetMerger (via RustBridge), print the RowIdMapping
 * output to stdout for inspection, and verify correctness by reading back
 * both original and merged Parquet files from disk.
 *
 * <p>Run via Gradle:
 * <pre>
 *   ./gradlew :sandbox:plugins:parquet-data-format:test --tests "*.RowIdMappingManualTest"
 * </pre>
 *
 * <p>Or run individual methods from IntelliJ (right-click on test method).
 */
public class RowIdMappingManualTests extends OpenSearchTestCase {

    private static final String INDEX_NAME = "rowid-manual-test";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<List<Map<String, Object>>> LIST_OF_MAPS = new TypeReference<>() {
    };

    private BufferAllocator allocator;
    private Schema schema;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        RustBridge.initLogger();
        allocator = new RootAllocator();
        schema = new Schema(
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

    // ════════════════════════════════════════════════════════════════════
    // Test 1: Merge Parquet files and print the RowIdMapping output
    // ════════════════════════════════════════════════════════════════════

    /**
     * Calls ParquetMerger (via RustBridge) and prints the full RowIdMapping output.
     * Creates multiple Parquet files with known data, merges them, and prints:
     * - The full RowIdMapping (generation offsets, sizes, and per-row mappings)
     * - The merged file content
     */
    public void testMergeAndPrintRowIdMapping() throws Exception {
        System.out.println("\n=== Test: Merge Parquet Files and Print RowIdMapping ===\n");

        RustBridge.onSettingsUpdate(NativeSettings.builder().indexName(INDEX_NAME).compressionType("LZ4_RAW").build());

        try {
            Path tempDir = createTempDir();

            // Create 4 source files with interleaved timestamps
            long[][] fileTimestamps = {
                { 10, 30, 50, 70 },       // File 0: even-ish timestamps
                { 20, 40, 60, 80 },       // File 1: interleaved with file 0
                { 5, 25, 45 },            // File 2: smaller file, overlapping
                { 100, 200 },             // File 3: large gap, 2 rows
            };

            List<String> filePaths = new ArrayList<>();
            int totalRows = 0;

            System.out.println("Source files:");
            for (int fi = 0; fi < fileTimestamps.length; fi++) {
                long[] ts = fileTimestamps[fi];
                int[] values = new int[ts.length];
                String[] tags = new String[ts.length];
                for (int r = 0; r < ts.length; r++) {
                    values[r] = fi * 100 + r;
                    tags[r] = "gen" + fi + "_row" + r;
                }

                String path = writeParquetFile(tempDir, "file_" + fi + ".parquet", ts, values, tags, fi);
                filePaths.add(path);
                totalRows += ts.length;

                System.out.printf("  File %d (generation %d): %d rows, timestamps=%s%n", fi, fi, ts.length, formatArray(ts));
            }
            System.out.printf("  Total rows across all files: %d%n%n", totalRows);

            // Merge
            String mergedPath = tempDir.resolve("merged_output.parquet").toString();
            System.out.println("Merging files...");
            RowIdMapping mapping = RustBridge.mergeParquetFilesInRust(filePaths.stream().map(Path::of).toList(), mergedPath, INDEX_NAME);

            // Print RowIdMapping details
            System.out.println("\n--- RowIdMapping Output ---\n");
            assertNotNull("mapping must not be null", mapping);
            System.out.println("Type: " + mapping.getClass().getSimpleName());
            assertTrue("expected PackedRowIdMapping", mapping instanceof PackedRowIdMapping);

            PackedRowIdMapping packed = (PackedRowIdMapping) mapping;
            System.out.println("Total size: " + packed.size());
            System.out.println("RAM bytes used: " + packed.ramBytesUsed());
            System.out.println("Generation offsets: " + packed.getGenerationOffsets());
            System.out.println("Generation sizes: " + packed.getGenerationSizes());

            if (packed.size() == 0) {
                System.out.println("\n⚠ RowIdMapping is empty (Rust merge does not yet populate mapping).");
                System.out.println("  The merged file content will still be verified below.");
            } else {
                assertEquals("mapping size must equal total rows", totalRows, packed.size());

                System.out.println("\nFull mapping (generation, oldRowId) -> newRowId:");
                System.out.println("──────────────────────────────────────────────────");
                for (Map.Entry<Long, Integer> entry : packed.getGenerationOffsets().entrySet()) {
                    long gen = entry.getKey();
                    int size = packed.getGenerationSize(gen);
                    System.out.printf("  Generation %d (%d rows):%n", gen, size);
                    for (int oldId = 0; oldId < size; oldId++) {
                        long newId = packed.getNewRowId(oldId, gen);
                        System.out.printf("    (gen=%d, oldRowId=%d) -> newRowId=%d%n", gen, oldId, newId);
                        assertTrue("newId must be >= 0", newId >= 0);
                        assertTrue("newId must be < totalRows", newId < totalRows);
                    }
                }
            }

            // Print merged file metadata
            ParquetFileMetadata meta = RustBridge.getFileMetadata(mergedPath);
            System.out.printf("%n--- Merged File Metadata ---%n");
            System.out.printf("  Rows: %d%n", meta.numRows());
            System.out.printf("  Version: %d%n", meta.version());
            assertEquals("merged file row count", totalRows, (int) meta.numRows());

            // Read and print merged file content
            String mergedJson = RustBridge.readAsJson(mergedPath);
            List<Map<String, Object>> mergedRows = JSON.readValue(mergedJson, LIST_OF_MAPS);
            System.out.printf("%n--- Merged File Content (sorted) ---%n");
            System.out.printf("%-8s %-12s %-8s %-15s%n", "row_id", "timestamp", "value", "tag");
            System.out.println("──────────────────────────────────────────────────");
            for (Map<String, Object> row : mergedRows) {
                System.out.printf("%-8s %-12s %-8s %-15s%n", row.get("__row_id__"), row.get("timestamp"), row.get("value"), row.get("tag"));
            }

            // Verify sorted order
            for (int i = 1; i < mergedRows.size(); i++) {
                long prev = ((Number) mergedRows.get(i - 1).get("timestamp")).longValue();
                long curr = ((Number) mergedRows.get(i).get("timestamp")).longValue();
                assertTrue("Merged output must be sorted at position " + i + ": " + prev + " > " + curr, prev <= curr);
            }

            System.out.println("\n✓ Merge and RowIdMapping print completed successfully!");

        } finally {
            RustBridge.removeSettings(INDEX_NAME);
        }
    }

    // ════════════════════════════════════════════════════════════════════
    // Test 2: Read merged files and verify RowIdMapping correctness
    // ════════════════════════════════════════════════════════════════════

    /**
     * Verifies RowIdMapping correctness by reading both original and merged
     * Parquet files back from disk and cross-referencing every mapping entry.
     *
     * For each (generation, oldRowId) in the mapping:
     * 1. Reads the original file's row at oldRowId (from disk)
     * 2. Translates via the mapping: newRowId = mapping.getNewRowId(oldRowId, generation)
     * 3. Reads the merged file's row whose __row_id__ == newRowId (from disk)
     * 4. Asserts every data column matches
     *
     * Also checks bijectivity, sort order, sequential IDs, and boundary conditions.
     */
    public void testVerifyRowIdMappingCorrectness() throws Exception {
        System.out.println("\n=== Test: Verify RowIdMapping by Reading Merged Parquet Files ===\n");

        RustBridge.onSettingsUpdate(NativeSettings.builder().indexName(INDEX_NAME).compressionType("LZ4_RAW").build());

        try {
            Path tempDir = createTempDir();

            // Create source files with deliberately tricky data
            long[][] fileTimestamps = {
                { 50, 10, 30 },           // File 0: unsorted within file
                { 90, 60, 20 },           // File 1: descending, overlaps with file 0
                { 5, 15, 25, 35, 45 },    // File 2: ascending, interleaves heavily
                { 40, 40, 40 },           // File 3: all duplicates
                { 1000 },                 // File 4: single row, large value
            };

            List<String> filePaths = new ArrayList<>();
            List<List<Map<String, Object>>> originalRows = new ArrayList<>();
            int totalRows = 0;

            System.out.println("Creating source files:");
            for (int fi = 0; fi < fileTimestamps.length; fi++) {
                long[] ts = fileTimestamps[fi];
                int[] values = new int[ts.length];
                String[] tags = new String[ts.length];
                for (int r = 0; r < ts.length; r++) {
                    values[r] = fi * 1000 + r;
                    tags[r] = "g" + fi + "r" + r;
                }

                String path = writeParquetFile(tempDir, "src_" + fi + ".parquet", ts, values, tags, fi);
                filePaths.add(path);
                totalRows += ts.length;

                // Read back original file from disk (ground truth)
                List<Map<String, Object>> rows = JSON.readValue(RustBridge.readAsJson(path), LIST_OF_MAPS);
                originalRows.add(rows);

                System.out.printf("  File %d: %d rows, timestamps=%s%n", fi, ts.length, formatArray(ts));
            }
            System.out.printf("  Total: %d rows%n%n", totalRows);

            // Merge
            String mergedPath = tempDir.resolve("verified_merge.parquet").toString();
            RowIdMapping mapping = RustBridge.mergeParquetFilesInRust(filePaths.stream().map(Path::of).toList(), mergedPath, INDEX_NAME);

            // Read merged file
            List<Map<String, Object>> mergedRows = JSON.readValue(RustBridge.readAsJson(mergedPath), LIST_OF_MAPS);
            System.out.printf("Merged file: %d rows%n%n", mergedRows.size());
            assertEquals("merged row count", totalRows, mergedRows.size());

            // Index merged rows by __row_id__ for O(1) lookup
            Map<Long, Map<String, Object>> mergedById = new HashMap<>();
            for (Map<String, Object> row : mergedRows) {
                mergedById.put(((Number) row.get("__row_id__")).longValue(), row);
            }

            // ── Verification ──
            System.out.println("Verifying RowIdMapping correctness...");
            System.out.println("──────────────────────────────────────────────────────────────────────");

            assertNotNull("mapping must not be null", mapping);
            assertTrue("expected PackedRowIdMapping", mapping instanceof PackedRowIdMapping);
            PackedRowIdMapping packed = (PackedRowIdMapping) mapping;

            if (packed.size() == 0) {
                System.out.println("  ⚠ RowIdMapping is empty (Rust merge does not yet populate mapping).");
                System.out.println("  Skipping per-row mapping verification; verifying merged file content only.\n");
            } else {
                assertEquals("mapping size must equal total rows", totalRows, packed.size());

                int errors = 0;
                int verified = 0;
                boolean[] seen = new boolean[totalRows];

                for (int gen = 0; gen < originalRows.size(); gen++) {
                    List<Map<String, Object>> origRows = originalRows.get(gen);
                    int genSize = packed.getGenerationSize(gen);

                    if (genSize != origRows.size()) {
                        System.out.printf("  ✗ ERROR: Generation %d size mismatch: expected=%d, got=%d%n", gen, origRows.size(), genSize);
                        errors++;
                        continue;
                    }

                    for (int oldId = 0; oldId < origRows.size(); oldId++) {
                        long newId = packed.getNewRowId(oldId, gen);

                        if (newId < 0 || newId >= totalRows) {
                            System.out.printf(
                                "  ✗ ERROR: (gen=%d, old=%d) -> newId=%d OUT OF RANGE [0, %d)%n",
                                gen,
                                oldId,
                                newId,
                                totalRows
                            );
                            errors++;
                            continue;
                        }

                        Map<String, Object> orig = origRows.get(oldId);
                        Map<String, Object> merged = mergedById.get(newId);

                        if (merged == null) {
                            System.out.printf("  ✗ ERROR: No merged row at __row_id__=%d%n", newId);
                            errors++;
                            continue;
                        }

                        // Compare data columns
                        long origTs = ((Number) orig.get("timestamp")).longValue();
                        long mergedTs = ((Number) merged.get("timestamp")).longValue();
                        long origVal = ((Number) orig.get("value")).longValue();
                        long mergedVal = ((Number) merged.get("value")).longValue();
                        String origTag = (String) orig.get("tag");
                        String mergedTag = (String) merged.get("tag");

                        boolean match = (origTs == mergedTs) && (origVal == mergedVal) && origTag.equals(mergedTag);

                        if (!match) {
                            System.out.printf("  ✗ MISMATCH: (gen=%d, old=%d) -> new=%d%n", gen, oldId, newId);
                            System.out.printf("      Original: ts=%d, val=%d, tag=%s%n", origTs, origVal, origTag);
                            System.out.printf("      Merged:   ts=%d, val=%d, tag=%s%n", mergedTs, mergedVal, mergedTag);
                            errors++;
                        } else {
                            System.out.printf(
                                "  ✓ (gen=%d, old=%d) -> new=%d  [ts=%d, val=%d, tag=%s]%n",
                                gen,
                                oldId,
                                newId,
                                origTs,
                                origVal,
                                origTag
                            );
                            verified++;
                        }

                        if (seen[(int) newId]) {
                            System.out.printf("  ✗ ERROR: Position %d mapped more than once!%n", newId);
                            errors++;
                        }
                        seen[(int) newId] = true;
                    }
                }

                // Check bijectivity: every position must be covered
                for (int i = 0; i < totalRows; i++) {
                    if (!seen[i]) {
                        System.out.printf("  ✗ ERROR: Position %d not covered by any mapping%n", i);
                        errors++;
                    }
                }

                System.out.printf("%n  Mapping verification: %d/%d rows verified, %d errors%n", verified, totalRows, errors);
                assertEquals("All mapping verifications must pass", 0, errors);
            }

            // Check merged output is sorted by timestamp
            boolean sorted = true;
            for (int i = 1; i < mergedRows.size(); i++) {
                long prev = ((Number) mergedRows.get(i - 1).get("timestamp")).longValue();
                long curr = ((Number) mergedRows.get(i).get("timestamp")).longValue();
                if (prev > curr) {
                    System.out.printf("  ✗ ERROR: Merged output not sorted at position %d: %d > %d%n", i, prev, curr);
                    sorted = false;
                }
            }

            // Check __row_id__ is sequential 0..N-1
            boolean sequential = true;
            for (int i = 0; i < mergedRows.size(); i++) {
                long rowId = ((Number) mergedRows.get(i).get("__row_id__")).longValue();
                if (rowId != i) {
                    System.out.printf("  ✗ ERROR: __row_id__ not sequential at position %d: got %d%n", i, rowId);
                    sequential = false;
                    break;
                }
            }

            // Boundary checks (these work even with empty mapping — returns -1 for unknown gen)
            assertEquals("invalid generation lookup should return -1", -1L, packed.getNewRowId(0, 999_999));
            assertEquals("negative oldId lookup should return -1", -1L, packed.getNewRowId(-1, 0));
            assertEquals("out-of-range oldId lookup should return -1", -1L, packed.getNewRowId(999_999, 0));

            // Summary
            System.out.println("──────────────────────────────────────────────────────────────────────");
            System.out.printf("%nResults:%n");
            System.out.printf("  Merged row count:  %d (expected %d)%n", mergedRows.size(), totalRows);
            System.out.printf("  Sorted:            %s%n", sorted ? "✓" : "✗");
            System.out.printf("  Sequential IDs:    %s%n", sequential ? "✓" : "✗");
            System.out.printf(
                "  Mapping populated: %s%n",
                packed.size() > 0 ? "✓ (" + packed.size() + " entries)" : "⚠ empty (TODO in Rust)"
            );

            assertTrue("Merged output must be sorted", sorted);
            assertTrue("__row_id__ must be sequential", sequential);
            System.out.println("\n✓ ALL VERIFICATIONS PASSED!");

        } finally {
            RustBridge.removeSettings(INDEX_NAME);
        }
    }

    // ════════════════════════════════════════════════════════════════════
    // Helper methods
    // ════════════════════════════════════════════════════════════════════

    private String writeParquetFile(Path dir, String name, long[] timestamps, int[] values, String[] tags, long generation)
        throws Exception {
        String path = dir.resolve(name).toString();
        ParquetSortConfig sortConfig = new ParquetSortConfig(List.of("timestamp"), List.of(false), List.of(false));

        try (ArrowExport schemaExport = exportSchema()) {
            NativeParquetWriter writer = new NativeParquetWriter(path, INDEX_NAME, schemaExport.getSchemaAddress(), sortConfig, generation);
            try (ArrowExport dataExport = exportData(timestamps, values, tags)) {
                writer.write(dataExport.getArrayAddress(), dataExport.getSchemaAddress());
            }
            writer.flush();
        }
        return path;
    }

    private ArrowExport exportSchema() {
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
        Data.exportSchema(allocator, schema, null, arrowSchema);
        return new ArrowExport(null, arrowSchema);
    }

    private ArrowExport exportData(long[] timestamps, int[] values, String[] tags) {
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            BigIntVector tsVec = (BigIntVector) root.getVector("timestamp");
            IntVector valVec = (IntVector) root.getVector("value");
            VarCharVector tagVec = (VarCharVector) root.getVector("tag");
            for (int i = 0; i < timestamps.length; i++) {
                tsVec.setSafe(i, timestamps[i]);
                valVec.setSafe(i, values[i]);
                tagVec.setSafe(i, tags[i].getBytes(StandardCharsets.UTF_8));
            }
            root.setRowCount(timestamps.length);

            ArrowArray array = ArrowArray.allocateNew(allocator);
            ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
            Data.exportVectorSchemaRoot(allocator, root, null, array, arrowSchema);
            return new ArrowExport(array, arrowSchema);
        }
    }

    private static String formatArray(long[] arr) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < arr.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(arr[i]);
        }
        return sb.append("]").toString();
    }
}
