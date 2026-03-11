package org.opensearch.index.engine.exec.merge;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.Merger;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.index.engine.exec.composite.CompositeDataFormatWriter.ROW_ID;

public class LuceneMerger implements Merger {

    private static final Logger logger = LogManager.getLogger(LuceneMerger.class);

    private static final Field SEGMENT_INFOS_FIELD;
    private static final Field SEGMENTS_FIELD;

    static {
        try {
            SEGMENT_INFOS_FIELD = IndexWriter.class.getDeclaredField("segmentInfos");
            SEGMENT_INFOS_FIELD.setAccessible(true);
            SEGMENTS_FIELD = SegmentInfos.class.getDeclaredField("segments");
            SEGMENTS_FIELD.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final CustomIndexWriter indexWriter;
    private final Path targetDirectoryPath;

    public LuceneMerger(CustomIndexWriter indexWriter, Path targetDirectoryPath) {
        this.indexWriter = indexWriter;
        this.targetDirectoryPath = targetDirectoryPath;
    }

    @Override
    public MergeResult merge(List<WriterFileSet> fileMetadataList, long writerGeneration) {
        return merge(fileMetadataList, null, writerGeneration);
    }

    @Override
    public MergeResult merge(List<WriterFileSet> fileMetadataList, RowIdMapping rowIdMapping, long writerGeneration) {
        try {
            if (rowIdMapping != null) {
                logger.info("LuceneMerger starting merge with RowIdMapping: fileId={}, mappingSize={}",
                    rowIdMapping.getFileId(), rowIdMapping.size());
            }

            // Collect the writer generations of the segments we want to merge.
            // These match the "writer_generation" attribute stored on each Lucene segment
            // by LuceneWriterCodec when the segment was originally written.
            Set<Long> generationsToMerge = new HashSet<>();
            for (WriterFileSet fileSet : fileMetadataList) {
                generationsToMerge.add(fileSet.getWriterGeneration());
            }

            // Get actual SegmentCommitInfo references from IndexWriter via reflection
            // and snapshot segment names before merge to identify the new output segment
            List<SegmentCommitInfo> segmentsToMerge = new ArrayList<>();
            Set<String> segmentNamesBefore = new HashSet<>();
            for (SegmentCommitInfo sci : getSegmentsViaReflection(indexWriter)) {
                segmentNamesBefore.add(sci.info.name);
                String generationAttr = sci.info.getAttribute("writer_generation");
                if (generationAttr != null && generationsToMerge.contains(Long.parseLong(generationAttr))) {
                    segmentsToMerge.add(sci);
                }
            }

            if (!segmentsToMerge.isEmpty()) {
                MergePolicy.OneMerge oneMerge = new CustomOneMerge(segmentsToMerge, rowIdMapping, writerGeneration);
                indexWriter.executeMerge(oneMerge);

                // Collect files from newly produced segment(s) using reflection
                Set<String> segmentFiles = new HashSet<>();
                for (SegmentCommitInfo sci : getSegmentsViaReflection(indexWriter)) {
                    if (!segmentNamesBefore.contains(sci.info.name)) {
                        segmentFiles.addAll(sci.files());
                    }
                }

                // Validate ROW_ID doc values against RowIdMapping (requires opening reader)
//               if (rowIdMapping != null) {
//                   validateRowIdMapping(rowIdMapping, segmentsToMerge, segmentNamesBefore);
//               }

                WriterFileSet mergedFileSet = WriterFileSet.builder()
                    .directory(targetDirectoryPath)
                    .writerGeneration(writerGeneration)
                    .addFiles(segmentFiles)
                    .build();

                Map<DataFormat, WriterFileSet> mergedWriterFileSet = Map.of(DataFormat.LUCENE, mergedFileSet);

                return new MergeResult(rowIdMapping, mergedWriterFileSet);
            }

            logger.info("Merged writer file set 2");

            // No segments to merge — return empty result
            return new MergeResult(rowIdMapping, Map.of());

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to access segments via reflection", e);
        }
    }

    @SuppressWarnings("unchecked")
    private List<SegmentCommitInfo> getSegmentsViaReflection(IndexWriter writer) throws IllegalAccessException {
        SegmentInfos segmentInfos = (SegmentInfos) SEGMENT_INFOS_FIELD.get(writer);
        return (List<SegmentCommitInfo>) SEGMENTS_FIELD.get(segmentInfos);
    }

    /**
     * Validates that the DocMap reordering was applied correctly.
     * Logs the merged segment's ___row_id values and mapping consistency.
     */
    private void validateRowIdMapping(RowIdMapping rowIdMapping, List<SegmentCommitInfo> segmentsToMerge,
                                      Set<String> segmentNamesBefore) throws IOException {
        // Build fileId -> baseDoc offset map (Lucene's concatenation order)
        Map<String, Integer> fileIdToBaseDoc = new HashMap<>();
        int baseDoc = 0;
        for (SegmentCommitInfo info : segmentsToMerge) {
            String writerGen = info.info.getAttribute("writer_generation");
            if (writerGen != null) {
                fileIdToBaseDoc.put(writerGen, baseDoc);
            }
            baseDoc += info.info.maxDoc();
        }

        logger.info("Validation fileIdToBaseDoc: {}", fileIdToBaseDoc);

        try (DirectoryReader reader = DirectoryReader.open(indexWriter)) {
            for (LeafReaderContext ctx : reader.leaves()) {
                SegmentReader sr = Lucene.segmentReader(ctx.reader());
                if (segmentNamesBefore.contains(sr.getSegmentInfo().info.name)) {
                    continue;
                }

                int totalDocs = sr.maxDoc();
                long[] mergedRowIds = new long[totalDocs];
                NumericDocValues rowIdDV = sr.getNumericDocValues(ROW_ID);
                if (rowIdDV == null) {
                    logger.debug("RowIdMapping validation skipped: no {} doc values in merged segment", ROW_ID);
                    return;
                }
                while (rowIdDV.nextDoc() != NumericDocValues.NO_MORE_DOCS) {
                    mergedRowIds[rowIdDV.docID()] = rowIdDV.longValue();
                }

                // Log merged row IDs for debugging
                StringBuilder sb = new StringBuilder("Merged ___row_id values: [");
                for (int i = 0; i < Math.min(totalDocs, 20); i++) {
                    if (i > 0) sb.append(", ");
                    sb.append(i).append("=").append(mergedRowIds[i]);
                }
                sb.append("]");
                logger.info(sb.toString());

                // Validate: for each file, the RowIdMapping says
                // (fileId, oldRowId) -> newRowId (position in merged parquet)
                // The DocMap reorders Lucene docs so that Lucene doc N = parquet row N
                // So after reorder, Lucene doc at newRowId should contain the data
                // from the original segment at (fileId, oldRowId)
                int matched = 0, mismatched = 0, skipped = 0;
                Map<String, Integer> fileOffsets = rowIdMapping.getFileOffsets();
                Map<String, Integer> fileSizes = rowIdMapping.getFileSizes();

                for (Map.Entry<String, Integer> entry : fileOffsets.entrySet()) {
                    String fileId = entry.getKey();
                    int offset = entry.getValue();
                    int size = fileSizes.getOrDefault(fileId, 0);
                    Integer segBase = fileIdToBaseDoc.get(fileId);

                    if (segBase == null) {
                        skipped += size;
                        continue;
                    }

                    for (int i = 0; i < size; i++) {
                        long newRowId = rowIdMapping.getNewRowIdAt(offset + i);
                        int oldDocId = segBase + i;

                        if (newRowId >= totalDocs || oldDocId >= totalDocs) {
                            mismatched++;
                            continue;
                        }

                        // After reorder, Lucene doc at newRowId should have the ___row_id
                        // that was originally at oldDocId. Since each original segment
                        // has ___row_id = [0, 1, 2, ...], the original ___row_id = i
                        // After DocMap reorder, the value at position newRowId should be i
                        long actualRowId = mergedRowIds[(int) newRowId];
                        if (actualRowId == i) {
                            matched++;
                        } else {
                            mismatched++;
                            if (mismatched <= 10) {
                                logger.warn("Validation MISMATCH: fileId={}, oldRowId={}, newPos={}, " +
                                    "expected ___row_id={}, actual ___row_id={}, oldDocId={}",
                                    fileId, i, newRowId, i, actualRowId, oldDocId);
                            }
                        }
                    }
                }
                logger.info("RowIdMapping validation: matched={}, mismatched={}, skipped={}", matched, mismatched, skipped);
            }
        }
    }
}
