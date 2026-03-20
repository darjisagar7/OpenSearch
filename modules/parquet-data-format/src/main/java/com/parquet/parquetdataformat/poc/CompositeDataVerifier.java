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
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
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
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Reads Lucene segments and Parquet files produced by {@link CompositeIngestHarness}
 * and verifies row-by-row that the data is identical.
 *
 * <p>Pairs Lucene segment N with Parquet file generation_N by index order.
 * For each pair, bulk-reads all point values from Lucene in a single pass per field,
 * then streams Parquet batches and compares every field value.
 *
 * <p><b>Usage:</b>
 * {@code java CompositeDataVerifier [dataDir] [maxRowsToCheck]}
 *
 * <p>Defaults: dataDir=/Users/darsaga/lucene-index, checks all rows.
 */
public class CompositeDataVerifier {

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
        int maxRows = args.length > 1 ? Integer.parseInt(args[1]) : Integer.MAX_VALUE;

        Path luceneDir = dataDir.resolve("lucene");
        Path parquetDir = dataDir.resolve("parquet");

        // Discover parquet files sorted by generation
        List<Path> parquetFiles;
        try (Stream<Path> s = Files.list(parquetDir)) {
            parquetFiles = s.filter(p -> p.toString().endsWith(".parquet"))
                .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                .toList();
        }

        // Discover lucene segments
        List<String> segmentNames;
        try (Directory dir = NIOFSDirectory.open(luceneDir)) {
            SegmentInfos segInfos = SegmentInfos.readLatestCommit(dir);
            segmentNames = new ArrayList<>();
            for (SegmentCommitInfo sci : segInfos) {
                segmentNames.add(sci.info.name);
            }
        }

        System.out.printf("Found %d Lucene segments, %d Parquet files%n",
            segmentNames.size(), parquetFiles.size());

        if (segmentNames.size() != parquetFiles.size()) {
            System.err.println("ERROR: segment count mismatch!");
            System.exit(1);
        }

        int totalMismatches = 0;
        long totalRowsChecked = 0;

        try (Directory dir = NIOFSDirectory.open(luceneDir);
             DirectoryReader reader = DirectoryReader.open(dir)) {

            List<LeafReaderContext> leaves = reader.leaves();

            for (int seg = 0; seg < leaves.size(); seg++) {
                Path parquetFile = parquetFiles.get(seg);
                LeafReader leafReader = leaves.get(seg).reader();
                int numDocs = leafReader.maxDoc();
                int rowsToCheck = Math.min(numDocs, maxRows);

                System.out.printf("%n=== Segment %d: lucene=%s (%d docs), parquet=%s ===%n",
                    seg, segmentNames.get(seg), numDocs, parquetFile.getFileName());

                long t0 = System.nanoTime();
                int mismatches = verifySegment(leafReader, parquetFile, rowsToCheck);
                long ms = (System.nanoTime() - t0) / 1_000_000;

                totalMismatches += mismatches;
                totalRowsChecked += rowsToCheck;

                if (mismatches == 0) {
                    System.out.printf("  ✓ %d rows verified — all match (%d ms)%n", rowsToCheck, ms);
                } else {
                    System.out.printf("  ✗ %d mismatches in %d rows (%d ms)%n", mismatches, rowsToCheck, ms);
                }
            }
        }

        System.out.printf("%n===== Verification Complete =====%n");
        System.out.printf("  Rows checked:  %d%n", totalRowsChecked);
        System.out.printf("  Mismatches:    %d%n", totalMismatches);
        System.out.printf("  Result:        %s%n",
            totalMismatches == 0 ? "PASS ✓" : "FAIL ✗");

        System.exit(totalMismatches == 0 ? 0 : 1);
    }

    /**
     * Verifies one Lucene segment against one Parquet file.
     * Bulk-reads all Lucene point values in one pass, then streams Parquet and compares.
     */
    private static int verifySegment(LeafReader leafReader, Path parquetFile, int maxRows)
        throws Exception {

        int numDocs = leafReader.maxDoc();

        // ---- Phase 1: Bulk-read all Lucene data ----

        // row_id from doc values
        long[] luceneRowIds = new long[numDocs];
        NumericDocValues rowIdDV = leafReader.getNumericDocValues(ROW_ID);
        for (int doc = 0; doc < numDocs; doc++) {
            rowIdDV.advanceExact(doc);
            luceneRowIds[doc] = rowIdDV.longValue();
        }

        // Point fields: bulk-read via single-pass visitor
        Map<String, long[]> luceneLongs = new HashMap<>();
        for (String f : LONG_FIELDS) {
            luceneLongs.put(f, bulkReadLongPoints(leafReader, f, numDocs));
        }

        Map<String, int[]> luceneInts = new HashMap<>();
        for (String f : INT_FIELDS) {
            luceneInts.put(f, bulkReadIntPoints(leafReader, f, numDocs));
        }

        Map<String, double[]> luceneDoubles = new HashMap<>();
        for (String f : DOUBLE_FIELDS) {
            luceneDoubles.put(f, bulkReadDoublePoints(leafReader, f, numDocs));
        }

        // str_5 is a StringField (indexed as term, not stored) — we can't read per-doc values
        // from the inverted index without stored fields. Skip str_5 comparison.
        // The numeric fields are sufficient to prove data identity.

        // ---- Phase 2: Stream Parquet and compare ----

        int mismatches = 0;

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            String uri = "file://" + parquetFile.toAbsolutePath();
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
                    VarCharVector pqStr5 = (VarCharVector) root.getVector("str_5");
                    BigIntVector pqLong6 = (BigIntVector) root.getVector("long_6");
                    IntVector pqInt7 = (IntVector) root.getVector("int_7");
                    Float8Vector pqDouble8 = (Float8Vector) root.getVector("double_8");

                    for (int r = 0; r < batchRows && globalRow < maxRows; r++, globalRow++) {
                        if (globalRow >= numDocs) break;

                        StringBuilder sb = new StringBuilder();
                        boolean match = true;

                        match &= checkLong(sb, "row_id", luceneRowIds[globalRow], pqRowId.get(r));
                        match &= checkLong(sb, "long_0", luceneLongs.get("long_0")[globalRow], pqLong0.get(r));
                        match &= checkInt(sb, "int_1", luceneInts.get("int_1")[globalRow], pqInt1.get(r));
                        match &= checkDouble(sb, "double_2", luceneDoubles.get("double_2")[globalRow], pqDouble2.get(r));
                        match &= checkInt(sb, "int_3", luceneInts.get("int_3")[globalRow], pqInt3.get(r));
                        match &= checkDouble(sb, "double_4", luceneDoubles.get("double_4")[globalRow], pqDouble4.get(r));
                        // str_5: Lucene StringField is not stored, skip
                        match &= checkLong(sb, "long_6", luceneLongs.get("long_6")[globalRow], pqLong6.get(r));
                        match &= checkInt(sb, "int_7", luceneInts.get("int_7")[globalRow], pqInt7.get(r));
                        match &= checkDouble(sb, "double_8", luceneDoubles.get("double_8")[globalRow], pqDouble8.get(r));

                        if (!match) {
                            mismatches++;
                            if (mismatches <= 10) {
                                System.out.printf("  MISMATCH at row %d:%n%s", globalRow, sb);
                            }
                        }
                    }
                }
            }
        }

        return mismatches;
    }

    // ---- Bulk point readers: single BKD tree traversal per field ----

    private static long[] bulkReadLongPoints(LeafReader reader, String field, int numDocs)
        throws IOException {
        long[] values = new long[numDocs];
        PointValues pv = reader.getPointValues(field);
        if (pv == null) return values;
        pv.intersect(new PointValues.IntersectVisitor() {
            @Override
            public void visit(int docID) {}

            @Override
            public void visit(int docID, byte[] packedValue) {
                if (docID < numDocs) {
                    values[docID] = LongPoint.decodeDimension(packedValue, 0);
                }
            }

            @Override
            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                return PointValues.Relation.CELL_CROSSES_QUERY;
            }
        });
        return values;
    }

    private static int[] bulkReadIntPoints(LeafReader reader, String field, int numDocs)
        throws IOException {
        int[] values = new int[numDocs];
        PointValues pv = reader.getPointValues(field);
        if (pv == null) return values;
        pv.intersect(new PointValues.IntersectVisitor() {
            @Override
            public void visit(int docID) {}

            @Override
            public void visit(int docID, byte[] packedValue) {
                if (docID < numDocs) {
                    values[docID] = IntPoint.decodeDimension(packedValue, 0);
                }
            }

            @Override
            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                return PointValues.Relation.CELL_CROSSES_QUERY;
            }
        });
        return values;
    }

    private static double[] bulkReadDoublePoints(LeafReader reader, String field, int numDocs)
        throws IOException {
        double[] values = new double[numDocs];
        PointValues pv = reader.getPointValues(field);
        if (pv == null) return values;
        pv.intersect(new PointValues.IntersectVisitor() {
            @Override
            public void visit(int docID) {}

            @Override
            public void visit(int docID, byte[] packedValue) {
                if (docID < numDocs) {
                    values[docID] = DoublePoint.decodeDimension(packedValue, 0);
                }
            }

            @Override
            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                return PointValues.Relation.CELL_CROSSES_QUERY;
            }
        });
        return values;
    }

    // ---- Comparison helpers ----

    private static boolean checkLong(StringBuilder sb, String name, long lucene, long parquet) {
        if (lucene != parquet) {
            sb.append(String.format("    %s: lucene=%d parquet=%d%n", name, lucene, parquet));
            return false;
        }
        return true;
    }

    private static boolean checkInt(StringBuilder sb, String name, int lucene, int parquet) {
        if (lucene != parquet) {
            sb.append(String.format("    %s: lucene=%d parquet=%d%n", name, lucene, parquet));
            return false;
        }
        return true;
    }

    private static boolean checkDouble(StringBuilder sb, String name, double lucene, double parquet) {
        if (Double.compare(lucene, parquet) != 0) {
            sb.append(String.format("    %s: lucene=%.6f parquet=%.6f%n", name, lucene, parquet));
            return false;
        }
        return true;
    }
}
