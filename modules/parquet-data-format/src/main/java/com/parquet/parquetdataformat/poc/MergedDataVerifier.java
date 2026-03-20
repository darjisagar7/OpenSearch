/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.poc;

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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Verifies the merged output from {@link CompositeMergeIndexSortHarness}.
 *
 * <p>Compares ___row_id ordering between the merged Lucene segment and the
 * merged Parquet file. Only ___row_id is compared since the numeric fields
 * (long_0, int_1, etc.) are only stored in Parquet, not in Lucene.
 *
 * <p><b>Usage:</b>
 * {@code java MergedDataVerifier [dataDir]}
 *
 * <p>Defaults: dataDir=/Users/darsaga/lucene-index
 */
public class MergedDataVerifier {

    private static final String ROW_ID = "___row_id";

    public static void main(String[] args) throws Exception {
        Path dataDir = args.length > 0
            ? Path.of(args[0])
            : Path.of("/Users/darsaga/lucene-index");

        Path mergedLuceneDir = dataDir.resolve("sorted_lucene_merged");
        Path mergedParquetDir = dataDir.resolve("sorted_parquet_merged");

        // Find the single merged parquet file
        Path mergedParquetFile;
        try (Stream<Path> s = Files.list(mergedParquetDir)) {
            mergedParquetFile = s.filter(p -> p.toString().endsWith(".parquet"))
                .findFirst()
                .orElseThrow(() -> new IOException("No parquet file in " + mergedParquetDir));
        }

        System.out.printf("Merged Lucene dir:    %s%n", mergedLuceneDir);
        System.out.printf("Merged Parquet file:  %s%n", mergedParquetFile);

        // Open Lucene and find the merged segment (largest by maxDoc)
        try (Directory dir = NIOFSDirectory.open(mergedLuceneDir);
             DirectoryReader reader = DirectoryReader.open(dir)) {

            LeafReaderContext mergedLeaf = reader.leaves().stream()
                .max(Comparator.comparingInt(l -> l.reader().maxDoc()))
                .orElseThrow();

            LeafReader leafReader = mergedLeaf.reader();
            int numDocs = leafReader.maxDoc();

            System.out.printf("Merged Lucene segment: %d docs%n", numDocs);

            // Read all Lucene ___row_id values
            System.out.println("\nReading Lucene ___row_id...");
            long t0 = System.nanoTime();

            long[] luceneRowIds = new long[numDocs];
            SortedNumericDocValues rowIdDV = leafReader.getSortedNumericDocValues(ROW_ID);
            for (int doc = 0; doc < numDocs; doc++) {
                rowIdDV.advanceExact(doc);
                luceneRowIds[doc] = rowIdDV.nextValue();
            }

            long luceneReadMs = (System.nanoTime() - t0) / 1_000_000;
            System.out.printf("Lucene read: %d ms%n", luceneReadMs);

            // Print first 20 Lucene row IDs
            System.out.print("  First 20 Lucene row IDs: ");
            for (int i = 0; i < Math.min(20, numDocs); i++) {
                System.out.printf("%d ", luceneRowIds[i]);
            }
            System.out.println();

            // Stream Parquet and compare ___row_id
            System.out.println("\nReading Parquet ___row_id and comparing...");
            long t1 = System.nanoTime();
            int mismatches = 0;
            int rowsChecked = 0;

            try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
                String uri = "file://" + mergedParquetFile.toAbsolutePath();
                ScanOptions scanOptions = new ScanOptions.Builder(Integer.MAX_VALUE)
                    .columns(Optional.of(new String[]{ROW_ID}))
                    .build();

                try (DatasetFactory factory = new FileSystemDatasetFactory(
                        allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
                     Dataset dataset = factory.finish();
                     Scanner scanner = dataset.newScan(scanOptions);
                     ArrowReader arrowReader = scanner.scanBatches()) {

                    int globalRow = 0;

                    while (arrowReader.loadNextBatch()) {
                        VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
                        int batchRows = root.getRowCount();

                        BigIntVector pqRowId = (BigIntVector) root.getVector(ROW_ID);

                        for (int r = 0; r < batchRows && globalRow < numDocs; r++, globalRow++) {
                            long luceneVal = luceneRowIds[globalRow];
                            long parquetVal = pqRowId.get(r);

                            if (luceneVal != parquetVal) {
                                mismatches++;
                                if (mismatches <= 10) {
                                    System.out.printf("  MISMATCH at doc %d: lucene=%d parquet=%d%n",
                                        globalRow, luceneVal, parquetVal);
                                }
                            }
                            rowsChecked++;
                        }
                    }
                }
            }

            long compareMs = (System.nanoTime() - t1) / 1_000_000;

            if (rowsChecked != numDocs) {
                System.out.printf("  WARNING: Parquet had %d rows, Lucene had %d docs%n",
                    rowsChecked, numDocs);
            }

            System.out.printf("%n===== Verification Complete =====%n");
            System.out.printf("  Rows checked:  %d%n", rowsChecked);
            System.out.printf("  Mismatches:    %d%n", mismatches);
            System.out.printf("  Lucene read:   %d ms%n", luceneReadMs);
            System.out.printf("  Compare:       %d ms%n", compareMs);
            System.out.printf("  Result:        %s%n",
                mismatches == 0 ? "PASS ✓" : "FAIL ✗");

            System.exit(mismatches == 0 ? 0 : 1);
        }
    }
}
