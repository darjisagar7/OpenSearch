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
import com.parquet.parquetdataformat.memory.ArrowBufferPool;
import com.parquet.parquetdataformat.vsr.ManagedVSR;
import com.parquet.parquetdataformat.vsr.VSRManager;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.common.settings.Settings;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

/**
 * Composite ingestion harness: writes identical data to both Lucene and Parquet.
 *
 * <p>Each row is generated once and written to both formats in the same loop,
 * guaranteeing that Lucene segment N and Parquet file N contain exactly the
 * same data. Each segment/file has {@code ___row_id} starting from 0.
 *
 * <p>Output layout:
 * <ul>
 *   <li>{@code outputDir/lucene/} — single directory with N Lucene segments</li>
 *   <li>{@code outputDir/parquet/} — N Parquet files</li>
 * </ul>
 *
 * <p><b>Usage:</b>
 * {@code java CompositeIngestHarness [outputDir] [numSegments] [rowsPerSegment]}
 *
 * <p>Defaults: 5 segments, 2,000,000 rows/segment.
 */
public class CompositeIngestHarness {

    private static final String ROW_ID = "___row_id";
    private static final String INDEX_NAME = "composite_bench";
    private static final Random RNG = new Random(42);
    private static final int NUM_KEYWORD_FIELDS = 10;
    private static final String TOKENIZED_FIELD = "text_content";

    static String randomWord(Random rand) {
        int len = 3 + rand.nextInt(10);
        char[] chars = new char[len];
        for (int i = 0; i < len; i++) chars[i] = (char) ('a' + rand.nextInt(26));
        return new String(chars);
    }

    public static void main(String[] args) throws Exception {
        Path outputDir = args.length > 0
            ? Path.of(args[0])
            : Path.of("/Users/darsaga/lucene-index");
        int numSegments = args.length > 1 ? Integer.parseInt(args[1]) : 5;
        int rowsPerSegment = args.length > 2 ? Integer.parseInt(args[2]) : 500_000;

        Path luceneDir = outputDir.resolve("sorted_lucene");
        Path parquetDir = outputDir.resolve("sorted_parquet");
        Files.createDirectories(luceneDir);
        Files.createDirectories(parquetDir);

        pushDefaultRustSettings();

        System.out.println("Output directory: " + outputDir);
        System.out.printf("Segments: %d, rows/segment: %d%n", numSegments, rowsPerSegment);

        Schema arrowSchema = buildArrowSchema();
        ArrowBufferPool arrowBufferPool = new ArrowBufferPool(Settings.EMPTY);

        long totalLuceneMs = 0;
        long totalParquetMs = 0;

        try (Directory dir = NIOFSDirectory.open(luceneDir)) {
            // Keyword field type: indexed, not tokenized, not stored
            org.apache.lucene.document.FieldType keywordFt = new org.apache.lucene.document.FieldType();
            keywordFt.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            keywordFt.setTokenized(false);
            keywordFt.setStored(false);
            keywordFt.setOmitNorms(true);
            keywordFt.freeze();

            // Text field type: indexed, tokenized, not stored
            org.apache.lucene.document.FieldType textFt = new org.apache.lucene.document.FieldType();
            textFt.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            textFt.setTokenized(true);
            textFt.setStored(false);
            textFt.setOmitNorms(false);
            textFt.freeze();

            String[] keywordFieldNames = new String[NUM_KEYWORD_FIELDS];
            for (int k = 0; k < NUM_KEYWORD_FIELDS; k++) keywordFieldNames[k] = "field_" + k;

            IndexWriterConfig iwc = new IndexWriterConfig();
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            iwc.setIndexSort(new Sort(new CompositeMergeIndexSortHarness.RowIdMappingSortField(ROW_ID)));
            iwc.setMergePolicy(NoMergePolicy.INSTANCE);
            iwc.setMergeScheduler(new SerialMergeScheduler());
            iwc.setRAMBufferSizeMB(10 * 1024);
            iwc.setMaxBufferedDocs(Integer.MAX_VALUE);
            iwc.setUseCompoundFile(false);

            try (IndexWriter luceneWriter = new IndexWriter(dir, iwc)) {
                for (int seg = 0; seg < numSegments; seg++) {
                    String parquetFile = parquetDir.resolve(
                        "_parquet_file_generation_" + seg + ".parquet").toString();

                    System.out.printf("%n=== Segment %d ===%n", seg);

                    RNG.setSeed(42L + seg * 1_000_000L);

                    VSRManager vsrManager = new VSRManager(
                        parquetFile, INDEX_NAME, arrowSchema, arrowBufferPool);

                    long luceneNs = 0;
                    long parquetNs = 0;

                    for (int i = 0; i < rowsPerSegment; i++) {
                        long rowId = i;

                        // ---- generate numeric data ----
                        long long_0 = RNG.nextLong();
                        int int_1 = RNG.nextInt(100_000);
                        double double_2 = RNG.nextDouble() * 1000;
                        int int_3 = RNG.nextInt(100_000);
                        double double_4 = RNG.nextDouble() * 1000;
                        String str_5 = "bench_" + i + "_" + Long.toHexString(RNG.nextLong());
                        long long_6 = RNG.nextLong();
                        int int_7 = RNG.nextInt(100_000);
                        double double_8 = RNG.nextDouble() * 1000;

                        // ---- generate keyword + text data ----
                        String[] kwValues = new String[NUM_KEYWORD_FIELDS];
                        for (int k = 0; k < NUM_KEYWORD_FIELDS; k++) {
                            StringBuilder sb = new StringBuilder();
                            int wordCount = 3 + RNG.nextInt(8);
                            for (int w = 0; w < wordCount; w++) {
                                if (w > 0) sb.append(' ');
                                sb.append(randomWord(RNG));
                            }
                            kwValues[k] = sb.toString();
                        }
                        StringBuilder textSb = new StringBuilder();
                        int textWords = 5 + RNG.nextInt(11);
                        for (int w = 0; w < textWords; w++) {
                            if (w > 0) textSb.append(' ');
                            textSb.append(randomWord(RNG));
                        }
                        String textValue = textSb.toString();

                        // ---- write to Lucene (keyword + text + doc values) ----
                        long t0 = System.nanoTime();
                        Document doc = new Document();
                        doc.add(new SortedNumericDocValuesField(ROW_ID, rowId));
                        for (int k = 0; k < NUM_KEYWORD_FIELDS; k++) {
                            doc.add(new org.apache.lucene.document.Field(
                                keywordFieldNames[k], kwValues[k], keywordFt));
                        }
                        doc.add(new org.apache.lucene.document.Field(
                            TOKENIZED_FIELD, textValue, textFt));
                        luceneWriter.addDocument(doc);
                        luceneNs += System.nanoTime() - t0;

                        // ---- write to Parquet (all data) ----
                        long t1 = System.nanoTime();
                        vsrManager.maybeRotateActiveVSR();
                        ManagedVSR vsr = vsrManager.getActiveManagedVSR();
                        int row = vsr.getRowCount();
                        ((BigIntVector) vsr.getVector(ROW_ID)).setSafe(row, rowId);
                        ((BigIntVector) vsr.getVector("long_0")).setSafe(row, long_0);
                        ((IntVector) vsr.getVector("int_1")).setSafe(row, int_1);
                        ((Float8Vector) vsr.getVector("double_2")).setSafe(row, double_2);
                        ((IntVector) vsr.getVector("int_3")).setSafe(row, int_3);
                        ((Float8Vector) vsr.getVector("double_4")).setSafe(row, double_4);
                        ((VarCharVector) vsr.getVector("str_5")).setSafe(row,
                            str_5.getBytes(StandardCharsets.UTF_8));
                        ((BigIntVector) vsr.getVector("long_6")).setSafe(row, long_6);
                        ((IntVector) vsr.getVector("int_7")).setSafe(row, int_7);
                        ((Float8Vector) vsr.getVector("double_8")).setSafe(row, double_8);
                        for (int k = 0; k < NUM_KEYWORD_FIELDS; k++) {
                            ((VarCharVector) vsr.getVector(keywordFieldNames[k])).setSafe(row,
                                kwValues[k].getBytes(StandardCharsets.UTF_8));
                        }
                        ((VarCharVector) vsr.getVector(TOKENIZED_FIELD)).setSafe(row,
                            textValue.getBytes(StandardCharsets.UTF_8));
                        vsr.setRowCount(row + 1);
                        parquetNs += System.nanoTime() - t1;
                    }

                    // flush Lucene segment
                    long t0 = System.nanoTime();
                    luceneWriter.commit();
                    luceneNs += System.nanoTime() - t0;

                    // flush Parquet file
                    long t1 = System.nanoTime();
                    vsrManager.flush(null);
                    vsrManager.close();
                    parquetNs += System.nanoTime() - t1;

                    long luceneMs = luceneNs / 1_000_000;
                    long parquetMs = parquetNs / 1_000_000;
                    totalLuceneMs += luceneMs;
                    totalParquetMs += parquetMs;

                    long parquetSize = Files.exists(Path.of(parquetFile))
                        ? Files.size(Path.of(parquetFile)) : 0;
                    System.out.printf("  Lucene:  %d rows, %d ms%n", rowsPerSegment, luceneMs);
                    System.out.printf("  Parquet: %d rows, %d MB, %d ms%n",
                        rowsPerSegment, parquetSize / (1024 * 1024), parquetMs);
                }
            }
        }

        arrowBufferPool.close();

        System.out.printf("%nLucene dir size:  %d MB%n", dirSize(luceneDir) / (1024 * 1024));
        System.out.printf("Parquet dir size: %d MB%n", dirSize(parquetDir) / (1024 * 1024));

        Path metaFile = outputDir.resolve("ingest_meta.properties");
        long totalRows = (long) numSegments * rowsPerSegment;
        Files.writeString(metaFile, String.join("\n",
            "numSegments=" + numSegments,
            "rowsPerSegment=" + rowsPerSegment,
            "totalRows=" + totalRows,
            "formats=lucene,parquet"
        ));

        System.out.println("\n===== Summary =====");
        System.out.printf("  Total rows:       %d%n", totalRows);
        System.out.printf("  Lucene total:     %d ms%n", totalLuceneMs);
        System.out.printf("  Parquet total:    %d ms%n", totalParquetMs);
        System.out.printf("  Metadata:         %s%n", metaFile);
    }

    private static Schema buildArrowSchema() {
        org.apache.arrow.vector.types.pojo.FieldType notNullLong =
            org.apache.arrow.vector.types.pojo.FieldType.notNullable(new ArrowType.Int(64, true));
        org.apache.arrow.vector.types.pojo.FieldType nullLong =
            org.apache.arrow.vector.types.pojo.FieldType.nullable(new ArrowType.Int(64, true));
        org.apache.arrow.vector.types.pojo.FieldType nullInt =
            org.apache.arrow.vector.types.pojo.FieldType.nullable(new ArrowType.Int(32, true));
        org.apache.arrow.vector.types.pojo.FieldType nullDouble =
            org.apache.arrow.vector.types.pojo.FieldType.nullable(
                new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
        org.apache.arrow.vector.types.pojo.FieldType nullUtf8 =
            org.apache.arrow.vector.types.pojo.FieldType.nullable(new ArrowType.Utf8());

        List<org.apache.arrow.vector.types.pojo.Field> fields = new java.util.ArrayList<>();
        fields.add(new org.apache.arrow.vector.types.pojo.Field(ROW_ID, notNullLong, null));
        fields.add(new org.apache.arrow.vector.types.pojo.Field("long_0", nullLong, null));
        fields.add(new org.apache.arrow.vector.types.pojo.Field("int_1", nullInt, null));
        fields.add(new org.apache.arrow.vector.types.pojo.Field("double_2", nullDouble, null));
        fields.add(new org.apache.arrow.vector.types.pojo.Field("int_3", nullInt, null));
        fields.add(new org.apache.arrow.vector.types.pojo.Field("double_4", nullDouble, null));
        fields.add(new org.apache.arrow.vector.types.pojo.Field("str_5", nullUtf8, null));
        fields.add(new org.apache.arrow.vector.types.pojo.Field("long_6", nullLong, null));
        fields.add(new org.apache.arrow.vector.types.pojo.Field("int_7", nullInt, null));
        fields.add(new org.apache.arrow.vector.types.pojo.Field("double_8", nullDouble, null));
        for (int k = 0; k < NUM_KEYWORD_FIELDS; k++) {
            fields.add(new org.apache.arrow.vector.types.pojo.Field("field_" + k, nullUtf8, null));
        }
        fields.add(new org.apache.arrow.vector.types.pojo.Field(TOKENIZED_FIELD, nullUtf8, null));
        return new Schema(fields);
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

    private static long dirSize(Path dir) throws IOException {
        try (Stream<Path> s = Files.walk(dir)) {
            return s.filter(Files::isRegularFile)
                .mapToLong(p -> {
                    try { return Files.size(p); } catch (IOException e) { return 0; }
                }).sum();
        }
    }
}
