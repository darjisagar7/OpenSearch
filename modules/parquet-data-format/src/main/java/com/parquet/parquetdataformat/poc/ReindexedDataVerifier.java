/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.poc;

import com.parquet.parquetdataformat.poc.CompositeReindexHarness;
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
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Verifies the reindexed output from {@link CompositeReindexHarness}.
 *
 * <p>Compares the reindexed Lucene index ({@code lucene_reindexed/}) against
 * the merged parquet file ({@code parquet_merged/}) row-by-row.
 *
 * <p><b>Usage:</b>
 * {@code java ReindexedDataVerifier [dataDir]}
 *
 * <p>Defaults: dataDir=/Users/darsaga/lucene-index
 */
public class ReindexedDataVerifier {

    private static final String ROW_ID = "___row_id";

    private static final String[] ALL_FIELDS = {
        ROW_ID, "long_0", "int_1", "double_2", "int_3",
        "double_4", "str_5", "long_6", "int_7", "double_8"
    };

    private static final String[] LONG_FIELDS = {"long_0", "long_6"};
    private static final String[] INT_FIELDS = {"int_1", "int_3", "int_7"};
    private static final String[] DOUBLE_FIELDS = {"double_2", "double_4", "double_8"};

    public static void main(String[] args) throws Exception {
        Path dataDir = args.length > 0
            ? Path.of(args[0])
            : Path.of("/Users/darsaga/lucene-index");

        Path reindexedLuceneDir = dataDir.resolve("lucene_reindexed");
        Path mergedParquetDir = dataDir.resolve("parquet_merged");

        Path mergedParquetFile;
        try (Stream<Path> s = Files.list(mergedParquetDir)) {
            mergedParquetFile = s.filter(p -> p.toString().endsWith(".parquet"))
                .findFirst()
                .orElseThrow(() -> new IOException("No parquet file in " + mergedParquetDir));
        }

        System.out.printf("Reindexed Lucene dir: %s%n", reindexedLuceneDir);
        System.out.printf("Merged Parquet file:  %s%n", mergedParquetFile);

        try (Directory dir = NIOFSDirectory.open(reindexedLuceneDir);
             DirectoryReader reader = DirectoryReader.open(dir)) {

            // Use the largest leaf (should be the only segment with NoMergePolicy)
            LeafReaderContext leaf = reader.leaves().stream()
                .max(Comparator.comparingInt(l -> l.reader().maxDoc()))
                .orElseThrow();

            LeafReader leafReader = leaf.reader();
            int numDocs = leafReader.maxDoc();

            System.out.printf("Reindexed Lucene: %d docs, %d segments%n",
                reader.numDocs(), reader.leaves().size());

            // Bulk-read Lucene data
            System.out.println("\nReading Lucene data...");
            long t0 = System.nanoTime();

            long[] luceneRowIds = new long[numDocs];
            NumericDocValues rowIdDV = leafReader.getNumericDocValues(ROW_ID);
            for (int doc = 0; doc < numDocs; doc++) {
                rowIdDV.advanceExact(doc);
                luceneRowIds[doc] = rowIdDV.longValue();
            }

            long[][] luceneLongs = new long[LONG_FIELDS.length][];
            for (int f = 0; f < LONG_FIELDS.length; f++) {
                luceneLongs[f] = bulkReadLongPoints(leafReader, LONG_FIELDS[f], numDocs);
            }

            int[][] luceneInts = new int[INT_FIELDS.length][];
            for (int f = 0; f < INT_FIELDS.length; f++) {
                luceneInts[f] = bulkReadIntPoints(leafReader, INT_FIELDS[f], numDocs);
            }

            double[][] luceneDoubles = new double[DOUBLE_FIELDS.length][];
            for (int f = 0; f < DOUBLE_FIELDS.length; f++) {
                luceneDoubles[f] = bulkReadDoublePoints(leafReader, DOUBLE_FIELDS[f], numDocs);
            }

            long luceneReadMs = (System.nanoTime() - t0) / 1_000_000;
            System.out.printf("Lucene read: %d ms%n", luceneReadMs);

            // Stream Parquet and compare
            System.out.println("Comparing with Parquet...");
            long t1 = System.nanoTime();
            int mismatches = 0;
            int rowsChecked = 0;

            try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
                String uri = "file://" + mergedParquetFile.toAbsolutePath();
                ScanOptions scanOptions = new ScanOptions.Builder(Integer.MAX_VALUE)
                    .columns(Optional.of(ALL_FIELDS))
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
                        BigIntVector pqLong0 = (BigIntVector) root.getVector("long_0");
                        IntVector pqInt1 = (IntVector) root.getVector("int_1");
                        Float8Vector pqDouble2 = (Float8Vector) root.getVector("double_2");
                        IntVector pqInt3 = (IntVector) root.getVector("int_3");
                        Float8Vector pqDouble4 = (Float8Vector) root.getVector("double_4");
                        BigIntVector pqLong6 = (BigIntVector) root.getVector("long_6");
                        IntVector pqInt7 = (IntVector) root.getVector("int_7");
                        Float8Vector pqDouble8 = (Float8Vector) root.getVector("double_8");

                        for (int r = 0; r < batchRows && globalRow < numDocs; r++, globalRow++) {
                            StringBuilder sb = new StringBuilder();
                            boolean match = true;

                            match &= cmp(sb, "row_id", luceneRowIds[globalRow], pqRowId.get(r));
                            match &= cmp(sb, "long_0", luceneLongs[0][globalRow], pqLong0.get(r));
                            match &= cmpInt(sb, "int_1", luceneInts[0][globalRow], pqInt1.get(r));
                            match &= cmpDbl(sb, "double_2", luceneDoubles[0][globalRow], pqDouble2.get(r));
                            match &= cmpInt(sb, "int_3", luceneInts[1][globalRow], pqInt3.get(r));
                            match &= cmpDbl(sb, "double_4", luceneDoubles[1][globalRow], pqDouble4.get(r));
                            match &= cmp(sb, "long_6", luceneLongs[1][globalRow], pqLong6.get(r));
                            match &= cmpInt(sb, "int_7", luceneInts[2][globalRow], pqInt7.get(r));
                            match &= cmpDbl(sb, "double_8", luceneDoubles[2][globalRow], pqDouble8.get(r));

                            if (!match) {
                                mismatches++;
                                if (mismatches <= 10) {
                                    System.out.printf("  MISMATCH at doc %d:%n%s", globalRow, sb);
                                }
                            }
                            rowsChecked++;
                        }
                    }
                }
            }

            long compareMs = (System.nanoTime() - t1) / 1_000_000;

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

    private static long[] bulkReadLongPoints(LeafReader reader, String field, int numDocs) throws IOException {
        long[] values = new long[numDocs];
        PointValues pv = reader.getPointValues(field);
        if (pv == null) return values;
        pv.intersect(new PointValues.IntersectVisitor() {
            @Override public void visit(int docID) {}
            @Override public void visit(int docID, byte[] packedValue) {
                if (docID < numDocs) values[docID] = LongPoint.decodeDimension(packedValue, 0);
            }
            @Override public PointValues.Relation compare(byte[] min, byte[] max) {
                return PointValues.Relation.CELL_CROSSES_QUERY;
            }
        });
        return values;
    }

    private static int[] bulkReadIntPoints(LeafReader reader, String field, int numDocs) throws IOException {
        int[] values = new int[numDocs];
        PointValues pv = reader.getPointValues(field);
        if (pv == null) return values;
        pv.intersect(new PointValues.IntersectVisitor() {
            @Override public void visit(int docID) {}
            @Override public void visit(int docID, byte[] packedValue) {
                if (docID < numDocs) values[docID] = IntPoint.decodeDimension(packedValue, 0);
            }
            @Override public PointValues.Relation compare(byte[] min, byte[] max) {
                return PointValues.Relation.CELL_CROSSES_QUERY;
            }
        });
        return values;
    }

    private static double[] bulkReadDoublePoints(LeafReader reader, String field, int numDocs) throws IOException {
        double[] values = new double[numDocs];
        PointValues pv = reader.getPointValues(field);
        if (pv == null) return values;
        pv.intersect(new PointValues.IntersectVisitor() {
            @Override public void visit(int docID) {}
            @Override public void visit(int docID, byte[] packedValue) {
                if (docID < numDocs) values[docID] = DoublePoint.decodeDimension(packedValue, 0);
            }
            @Override public PointValues.Relation compare(byte[] min, byte[] max) {
                return PointValues.Relation.CELL_CROSSES_QUERY;
            }
        });
        return values;
    }

    private static boolean cmp(StringBuilder sb, String name, long a, long b) {
        if (a != b) { sb.append(String.format("    %s: lucene=%d parquet=%d%n", name, a, b)); return false; }
        return true;
    }

    private static boolean cmpInt(StringBuilder sb, String name, int a, int b) {
        if (a != b) { sb.append(String.format("    %s: lucene=%d parquet=%d%n", name, a, b)); return false; }
        return true;
    }

    private static boolean cmpDbl(StringBuilder sb, String name, double a, double b) {
        if (Double.compare(a, b) != 0) { sb.append(String.format("    %s: lucene=%.6f parquet=%.6f%n", name, a, b)); return false; }
        return true;
    }
}
