/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Merge profiling harness. Reads Lucene segments created by {@link MergeIngestHarness},
 * builds a synthetic RowIdMapping (identity mapping, same as Rust would produce for
 * a simple concatenation merge), then runs Lucene merge via CustomOneMerge with
 * RowIdMapping-based doc reorder.
 *
 * Usage:
 *   java MergeBenchHarness dataDir
 *
 * where dataDir is the output of MergeIngestHarness (contains lucene/ subdir
 * and ingest_meta.properties).
 *
 * Attach async-profiler / JFR to this process for flame graphs.
 */
public class MergeBenchHarness {

    private static final String ROW_ID = "___row_id";

    public static void main(String[] args) throws Exception {
//        if (args.length < 1) {
//            System.err.println("Usage: java MergeBenchHarness <dataDir>");
//            System.exit(1);
//        }
        Path outputDir = args.length > 0
            ? Path.of(args[0])
            : Path.of("/Users/darsaga/lucene-index");

        Path dataDir = Path.of(args[0]);
        Path luceneDir = dataDir.resolve("lucene");
        Path metaFile = dataDir.resolve("ingest_meta.properties");

        if (!Files.isDirectory(luceneDir)) {
            System.err.println("Expected lucene/ subdir in: " + dataDir);
            System.exit(1);
        }

        // Read metadata
        int numSegments, rowsPerSegment;
        long totalRows;
        if (Files.exists(metaFile)) {
            Properties props = new Properties();
            props.load(Files.newBufferedReader(metaFile));
            numSegments = Integer.parseInt(props.getProperty("numSegments"));
            rowsPerSegment = Integer.parseInt(props.getProperty("rowsPerSegment"));
            totalRows = Long.parseLong(props.getProperty("totalRows"));
        } else {
            System.err.println("Warning: ingest_meta.properties not found, auto-detecting...");
            try (Stream<Path> s = Files.list(luceneDir)) {
                numSegments = (int) s.filter(Files::isDirectory).count();
            }
            rowsPerSegment = 2_000_000;
            totalRows = (long) numSegments * rowsPerSegment;
        }

        System.out.printf("Data: %d segments, %d rows/segment, %d total rows%n",
            numSegments, rowsPerSegment, totalRows);

        // Discover segment directories
        List<Path> segDirs;
        try (Stream<Path> stream = Files.list(luceneDir)) {
            segDirs = stream.filter(Files::isDirectory).sorted().collect(Collectors.toList());
        }

        // ========== Phase 1: Build synthetic RowIdMapping ==========
        // This mimics what Rust produces: an identity mapping where files are
        // processed in order and newRowId = sequential position in output.
        System.out.println("\n===== Phase 1: Building synthetic RowIdMapping =====");
        long buildStart = System.nanoTime();
        RowIdMapping rowIdMapping = buildSyntheticRowIdMapping(numSegments, rowsPerSegment);
        long buildMs = (System.nanoTime() - buildStart) / 1_000_000;
        System.out.printf("  RowIdMapping built: %d entries, %d files, %d ms%n",
            rowIdMapping.size(), rowIdMapping.getFileOffsets().size(), buildMs);

        // ========== Phase 2: Lucene Merge with RowIdMapping ==========
        System.out.println("\n===== Phase 2: Lucene Merge (CustomOneMerge + RowIdMapping reorder) =====");

        Path combinedDir = dataDir.resolve("lucene_combined");
        Files.createDirectories(combinedDir);

        long p2Start = System.nanoTime();

        try (Directory combined = NIOFSDirectory.open(combinedDir)) {
            IndexWriterConfig iwc = new IndexWriterConfig();
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            iwc.setIndexSort(new Sort(new SortField(ROW_ID, SortField.Type.LONG)));
            iwc.setMergePolicy(NoMergePolicy.INSTANCE);
            iwc.setMergeScheduler(new SerialMergeScheduler());

            try (CustomIndexWriter writer = new CustomIndexWriter(combined, iwc)) {
                // Add all segment directories
                List<Directory> dirs = new ArrayList<>();
                for (Path segDir : segDirs) {
                    dirs.add(NIOFSDirectory.open(segDir));
                }
                writer.addIndexes(dirs.toArray(new Directory[0]));
                writer.commit();

                long addMs = (System.nanoTime() - p2Start) / 1_000_000;
                System.out.printf("  addIndexes: %d segments, %d ms%n", dirs.size(), addMs);

                // Collect segments and print info
                SegmentInfos segInfos = SegmentInfos.readLatestCommit(combined);
                List<SegmentCommitInfo> allSegments = new ArrayList<>(segInfos.asList());

                System.out.printf("  Segments in combined dir: %d%n", allSegments.size());
                for (SegmentCommitInfo sci : allSegments) {
                    System.out.printf("    segment=%s, maxDoc=%d, writer_generation=%s%n",
                        sci.info.name, sci.info.maxDoc(),
                        sci.info.getAttribute("writer_generation"));
                }

                // Execute merge with RowIdMapping reorder
                long mergeStart = System.nanoTime();
                CustomOneMerge oneMerge = new CustomOneMerge(allSegments, rowIdMapping);
                writer.executeMerge(oneMerge);
                writer.commit();
                long mergeMs = (System.nanoTime() - mergeStart) / 1_000_000;

                // Report
                SegmentInfos afterMerge = SegmentInfos.readLatestCommit(combined);
                System.out.printf("  Lucene merge: %d ms%n", mergeMs);
                System.out.printf("  Segments after merge: %d%n", afterMerge.size());

                try (DirectoryReader reader = DirectoryReader.open(combined)) {
                    System.out.printf("  Total docs after merge: %d%n", reader.numDocs());
                }

                for (Directory d : dirs) {
                    d.close();
                }
            }
        }

        long p2TotalMs = (System.nanoTime() - p2Start) / 1_000_000;

        // ========== Summary ==========
        System.out.println("\n===== Summary =====");
        System.out.printf("  RowIdMapping build: %d ms%n", buildMs);
        System.out.printf("  Lucene merge total: %d ms%n", p2TotalMs);
        System.out.printf("  RowIdMapping: %d entries, %d files%n",
            rowIdMapping.size(), rowIdMapping.getFileOffsets().size());
    }

    /**
     * Builds a synthetic RowIdMapping identical to what Rust produces for a
     * simple concatenation merge: files processed in order 0..N-1, each row's
     * newRowId = its sequential position in the output.
     *
     * fileOffsets: {"0" -> 0, "1" -> rowsPerSegment, "2" -> 2*rowsPerSegment, ...}
     * mapping[i] = i  (identity — row i in input becomes row i in output)
     */
    private static RowIdMapping buildSyntheticRowIdMapping(int numSegments, int rowsPerSegment) {
        long totalRows = (long) numSegments * rowsPerSegment;
        long[] mapping = new long[(int) totalRows];
        Map<String, Integer> fileOffsets = new HashMap<>();
        Map<String, Integer> fileSizes = new HashMap<>();

        int offset = 0;
        for (int seg = 0; seg < numSegments; seg++) {
            String fileId = String.valueOf(seg);
            fileOffsets.put(fileId, offset);
            fileSizes.put(fileId, rowsPerSegment);

            for (int row = 0; row < rowsPerSegment; row++) {
                mapping[offset + row] = offset + row;
            }
            offset += rowsPerSegment;
        }

        return new RowIdMapping(mapping, fileOffsets, fileSizes, "merged_99");
    }
}
