/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.index.engine.exec.lucene.writer.LuceneWriterCodec;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Ingestion harness: creates N Lucene segments with matching writer_generation
 * and ___row_id NumericDocValues, ready for merge profiling.
 *
 * Each segment is written to its own subdirectory under outputDir/lucene/{generation}/
 * with a LuceneWriterCodec that stamps writer_generation on the segment.
 *
 * Usage:
 *   java MergeIngestHarness [outputDir] [numSegments] [rowsPerSegment]
 *
 * Defaults: 5 segments, 2_000_000 rows/segment (~200MB each)
 */
public class MergeIngestHarness {

    private static final String ROW_ID = "___row_id";
    private static final Random RNG = new Random(42);

    public static void main(String[] args) throws Exception {
        Path outputDir = Paths.get("/Users/darsaga/composite");
//        Path outputDir = args.length > 0
//            ? Path.of(args[0])
//            : Path.of(System.getProperty("java.io.tmpdir"), "merge_bench_" + System.currentTimeMillis());
        int numSegments = args.length > 1 ? Integer.parseInt(args[1]) : 5;
        int rowsPerSegment = args.length > 2 ? Integer.parseInt(args[2]) : 2_000_000;

        Path luceneDir = outputDir.resolve("lucene");
        Files.createDirectories(luceneDir);

        System.out.println("Output directory: " + outputDir);
        System.out.println("Segments: " + numSegments + ", rows/segment: " + rowsPerSegment);

        long globalRowId = 0;

        for (int seg = 0; seg < numSegments; seg++) {
            long gen = seg;
            Path segDir = luceneDir.resolve(String.valueOf(gen));
            Files.createDirectories(segDir);

            System.out.printf("=== Segment %d (generation=%d) ===%n", seg, gen);

            long start = System.nanoTime();
            ingestLuceneSegment(segDir, gen, globalRowId, rowsPerSegment);
            long ms = (System.nanoTime() - start) / 1_000_000;

            long sizeBytes = Files.walk(segDir).filter(Files::isRegularFile)
                .mapToLong(p -> { try { return Files.size(p); } catch (IOException e) { return 0; } }).sum();

            System.out.printf("  Lucene: %d rows, %d MB, %d ms%n",
                rowsPerSegment, sizeBytes / (1024 * 1024), ms);

            globalRowId += rowsPerSegment;
        }

        // Write metadata for the merge harness
        Path metaFile = outputDir.resolve("ingest_meta.properties");
        Files.writeString(metaFile, String.join("\n",
            "numSegments=" + numSegments,
            "rowsPerSegment=" + rowsPerSegment,
            "totalRows=" + globalRowId
        ));

        System.out.println("\nIngestion complete. Total rows: " + globalRowId);
        System.out.println("Lucene dir: " + luceneDir);
        System.out.println("Metadata:   " + metaFile);
    }

    private static void ingestLuceneSegment(Path segDir, long generation, long startRowId, int rowCount)
        throws IOException {

        try (Directory dir = NIOFSDirectory.open(segDir)) {
            IndexWriterConfig iwc = new IndexWriterConfig();
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            iwc.setIndexSort(new Sort(new SortField(ROW_ID, SortField.Type.LONG)));
            iwc.setMergePolicy(NoMergePolicy.INSTANCE);
            iwc.setMergeScheduler(new SerialMergeScheduler());
            iwc.setCodec(new LuceneWriterCodec("default", iwc.getCodec(), generation));

            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                long rowId = startRowId;
                for (int i = 0; i < rowCount; i++) {
                    Document doc = new Document();
                    doc.add(new NumericDocValuesField(ROW_ID, rowId));
                    doc.add(new LongPoint("long_0", RNG.nextLong()));
                    doc.add(new IntPoint("int_1", RNG.nextInt(100_000)));
                    doc.add(new DoublePoint("double_2", RNG.nextDouble() * 1000));
                    doc.add(new IntPoint("int_3", RNG.nextInt(100_000)));
                    doc.add(new DoublePoint("double_4", RNG.nextDouble() * 1000));
                    doc.add(new StringField("str_5",
                        "bench_" + i + "_" + Long.toHexString(RNG.nextLong()), Field.Store.NO));
                    doc.add(new LongPoint("long_6", RNG.nextLong()));
                    doc.add(new IntPoint("int_7", RNG.nextInt(100_000)));
                    doc.add(new DoublePoint("double_8", RNG.nextDouble() * 1000));
                    writer.addDocument(doc);
                    rowId++;
                }
                writer.forceMerge(1);
                writer.commit();
            }
        }
    }
}
