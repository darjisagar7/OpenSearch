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
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;

import org.opensearch.index.engine.exec.merge.RowIdMapping;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Reindex harness: merges parquet files first, then reads the merged parquet
 * file and re-ingests rows into a fresh Lucene index.
 *
 * <p>Flow:
 * <ol>
 *   <li><b>Phase 1 — Parquet merge</b>: merges the per-segment parquet files
 *       from {@code dataDir/parquet/} into a single file in
 *       {@code dataDir/parquet_merged/} using {@code RustBridge}.
 *       Also produces a {@code RowIdMapping} for row reordering.</li>
 *   <li><b>Phase 2 — Lucene reindex</b>: reads keyword/text fields from the
 *       merged parquet and writes a fresh Lucene index at
 *       {@code dataDir/lucene_reindexed/}, using the RowIdMapping to assign
 *       new row IDs.</li>
 * </ol>
 *
 * <p>This provides an alternative to {@link CompositeMergeHarness}'s Lucene
 * merge approach. Instead of merging existing Lucene segments with RowIdMapping
 * reorder, this reads the merged parquet directly and writes new Lucene docs.
 * Compare timings between the two to evaluate merge vs reindex performance.
 *
 * <p><b>Usage:</b>
 * {@code java CompositeReindexHarness [dataDir]}
 *
 * <p>Expects per-segment parquet files at {@code dataDir/parquet/}.
 * Outputs merged parquet at {@code dataDir/parquet_merged/} and a new
 * Lucene index at {@code dataDir/lucene_reindexed/}.
 *
 * <p>Defaults: dataDir=/Users/darsaga/lucene-index
 */
public class CompositeReindexHarness {

    private static final String ROW_ID = "___row_id";

    private static final String INDEX_NAME = "composite_bench";
    private static final int NUM_KEYWORD_FIELDS = 10;
    private static final String TOKENIZED_FIELD = "text_content";

    public static void main(String[] args) throws Exception {
        Path dataDir = args.length > 0
            ? Path.of(args[0])
            : Path.of("/Users/darsaga/lucene-index");

        Path parquetDir = dataDir.resolve("parquet");
        Path mergedParquetDir = dataDir.resolve("parquet_merged_reindexed");
        Path reindexedLuceneDir = dataDir.resolve("lucene_reindexed");

        Files.createDirectories(mergedParquetDir);
        Files.createDirectories(reindexedLuceneDir);

        pushDefaultRustSettings();

        // ========== Phase 1: Parquet merge ==========
        System.out.println("===== Phase 1: Parquet merge (Rust) =====");

        List<Path> parquetFiles;
        try (Stream<Path> s = Files.list(parquetDir)) {
            parquetFiles = s.filter(p -> p.toString().endsWith(".parquet"))
                .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                .toList();
        }
        System.out.printf("  Input parquet files: %d%n", parquetFiles.size());

        String mergedParquetPath = mergedParquetDir.resolve(
            "_parquet_file_generation_merged_99.parquet").toString();

        long p1Start = System.nanoTime();
        RowIdMapping rowIdMapping = RustBridge.mergeParquetFilesInRust(
            parquetFiles, mergedParquetPath, INDEX_NAME);
        long p1Ms = (System.nanoTime() - p1Start) / 1_000_000;

        Path mergedParquetFile = Path.of(mergedParquetPath);
        long mergedParquetSize = Files.exists(mergedParquetFile)
            ? Files.size(mergedParquetFile) : 0;

        System.out.printf("  Parquet merge time: %d ms%n", p1Ms);
        System.out.printf("  Merged file:        %s (%d MB)%n",
            mergedParquetFile, mergedParquetSize / (1024 * 1024));
        System.out.printf("  RowIdMapping:       %d entries%n", rowIdMapping.size());

        // ========== Phase 2: Reindex: read merged parquet → write Lucene ==========
        System.out.println("\n===== Phase 2: Reindex: parquet → Lucene =====");

        long startNs = System.nanoTime();
        long docsIndexed = 0;

        try (Directory dir = NIOFSDirectory.open(reindexedLuceneDir)) {
            IndexWriterConfig iwc = new IndexWriterConfig();
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            iwc.setMergePolicy(NoMergePolicy.INSTANCE);
            iwc.setMergeScheduler(new SerialMergeScheduler());

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

            // Build list of columns to read: ___row_id + keyword fields + text field
            String[] readColumns = new String[1 + NUM_KEYWORD_FIELDS + 1];
            readColumns[0] = ROW_ID;
            for (int k = 0; k < NUM_KEYWORD_FIELDS; k++) readColumns[1 + k] = keywordFieldNames[k];
            readColumns[readColumns.length - 1] = TOKENIZED_FIELD;

            try (IndexWriter writer = new IndexWriter(dir, iwc);
                 BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {

                String uri = "file://" + mergedParquetFile.toAbsolutePath();
                ScanOptions scanOptions = new ScanOptions.Builder(Integer.MAX_VALUE)
                    .columns(Optional.of(readColumns))
                    .build();

                // The merged parquet has rows in the new order.
                // Row at position i in the merged file gets ___row_id = i.
                long globalRow = 0;

                try (DatasetFactory factory = new FileSystemDatasetFactory(
                        allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
                     Dataset dataset = factory.finish();
                     Scanner scanner = dataset.newScan(scanOptions);
                     ArrowReader arrowReader = scanner.scanBatches()) {

                    while (arrowReader.loadNextBatch()) {
                        VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
                        int batchRows = root.getRowCount();
                        BigIntVector vRowId = (BigIntVector) root.getVector(ROW_ID);

                        VarCharVector[] kwVectors = new VarCharVector[NUM_KEYWORD_FIELDS];
                        for (int k = 0; k < NUM_KEYWORD_FIELDS; k++) {
                            kwVectors[k] = (VarCharVector) root.getVector(keywordFieldNames[k]);
                        }
                        VarCharVector vText = (VarCharVector) root.getVector(TOKENIZED_FIELD);

                        Document doc = new Document();
                        for (int r = 0; r < batchRows; r++) {
                            // Use the row_id from the merged parquet (already reordered)
                            doc.add(new SortedNumericDocValuesField(ROW_ID, vRowId.get(r)));
                            for (int k = 0; k < NUM_KEYWORD_FIELDS; k++) {
                                doc.add(new org.apache.lucene.document.Field(
                                    keywordFieldNames[k],
                                    new String(kwVectors[k].get(r), StandardCharsets.UTF_8),
                                    keywordFt));
                            }
                            doc.add(new org.apache.lucene.document.Field(
                                TOKENIZED_FIELD,
                                new String(vText.get(r), StandardCharsets.UTF_8),
                                textFt));
                            writer.addDocument(doc);
                            doc.clear();
                            globalRow++;
                        }
                        docsIndexed += batchRows;
                    }
                }

                writer.commit();
            }
        }

        long totalMs = (System.nanoTime() - startNs) / 1_000_000;

        // ========== Verify ==========
        long luceneSize = dirSize(reindexedLuceneDir);
        int luceneDocs;
        try (Directory dir = NIOFSDirectory.open(reindexedLuceneDir);
             DirectoryReader reader = DirectoryReader.open(dir)) {
            luceneDocs = reader.numDocs();
        }

        // ========== Summary ==========
        System.out.println("\n===== Summary =====");
        System.out.printf("  Parquet merge time: %d ms%n", p1Ms);
        System.out.printf("  Reindex time:       %d ms%n", totalMs);
        System.out.printf("  Total time:         %d ms%n", p1Ms + totalMs);
        System.out.printf("  Docs indexed:       %d%n", docsIndexed);
        System.out.printf("  Lucene docs:        %d%n", luceneDocs);
        System.out.printf("  Merged parquet:     %d MB%n", mergedParquetSize / (1024 * 1024));
        System.out.printf("  Lucene dir size:    %d MB%n", luceneSize / (1024 * 1024));
        System.out.printf("  Output:             %s%n", reindexedLuceneDir);
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
