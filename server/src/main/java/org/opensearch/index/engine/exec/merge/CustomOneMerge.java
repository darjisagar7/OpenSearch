package org.opensearch.index.engine.exec.merge;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

public class CustomOneMerge extends MergePolicy.OneMerge {

    private static final Logger logger = LogManager.getLogger(CustomOneMerge.class);

    private final RowIdMapping rowIdMapping;
    private final long writerGeneration;

    public CustomOneMerge(
        List<SegmentCommitInfo> segments,
        RowIdMapping rowIdMapping,
        long writerGeneration
    ) {
        super(segments);
        this.rowIdMapping = rowIdMapping;
        this.writerGeneration = writerGeneration;
    }

    @Override
    public Sorter.DocMap reorder(CodecReader reader, Directory dir, Executor executor) throws IOException {
        int totalDocs = reader.maxDoc();

        // Build oldToNew: maps current doc position -> new position after reorder
        // Build newToOld: maps new position -> original doc position
        // Use PackedLongValues for memory efficiency (2-3 bytes per entry vs 4 bytes for int[])
        PackedLongValues.Builder oldToNewBuilder = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
//        PackedLongValues.Builder newToOldBuilder = PackedLongValues.packedBuilder(PackedInts.DEFAULT);

        // Initialize with identity mapping (each doc maps to itself by default)
        for (int i = 0; i < totalDocs; i++) {
            oldToNewBuilder.add(i);
//            newToOldBuilder.add(i);
        }

        // Track doc offset per segment, keyed by writer_generation attribute
        Map<String, Integer> fileIdToBaseDoc = new HashMap<>();
        int baseDoc = 0;
        for (SegmentCommitInfo segmentInfo : segments) {
            String writerGen = segmentInfo.info.getAttribute("writer_generation");
            if (writerGen != null) {
                fileIdToBaseDoc.put(writerGen, baseDoc);
            }
            baseDoc += segmentInfo.info.maxDoc();
        }

        // Build temporary arrays for mapping (we'll compress them after)
        int[] oldToNewArray = new int[totalDocs];
//        int[] newToOldArray = new int[totalDocs];

        // Initialize with identity mapping
        for (int i = 0; i < totalDocs; i++) {
            oldToNewArray[i] = i;
//            newToOldArray[i] = i;
        }

        // Build mapping using fileSizes for correct iteration
        Map<String, Integer> fileOffsets = rowIdMapping.getFileOffsets();
        Map<String, Integer> fileSizes = rowIdMapping.getFileSizes();

        for (Map.Entry<String, Integer> entry : fileOffsets.entrySet()) {
            String fileId = entry.getKey();
            Integer segmentBase = fileIdToBaseDoc.get(fileId);
            if (segmentBase == null) {
                continue;
            }

            int offset = entry.getValue();
            int size = fileSizes.getOrDefault(fileId, 0);

            // Process all mappings for this file
            for (int i = 0; i < size; i++) {
                int oldDocId = segmentBase + i;
                int newDocId = (int) rowIdMapping.getNewRowIdAt(offset + i);

                if (oldDocId < totalDocs && newDocId < totalDocs) {
                    oldToNewArray[oldDocId] = newDocId;
//                    newToOldArray[newDocId] = oldDocId;
                }
            }
        }

        // Compress into PackedLongValues for memory efficiency
        oldToNewBuilder = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
//        newToOldBuilder = PackedLongValues.packedBuilder(PackedInts.DEFAULT);

        for (int i = 0; i < totalDocs; i++) {
            oldToNewBuilder.add(oldToNewArray[i]);
//            newToOldBuilder.add(newToOldArray[i]);
        }

        PackedLongValues oldToNew = oldToNewBuilder.build();
//        PackedLongValues newToOld = newToOldBuilder.build();

        return new Sorter.DocMap() {
            @Override
            public int oldToNew(int docID) {
                return (int) oldToNew.get(docID);
            }

            @Override
            public int newToOld(int docID) {
                return 0;
            }

            @Override
            public int size() {
                return totalDocs;
            }
        };
    }

    @Override
    public void setMergeInfo(SegmentCommitInfo info) {
        super.setMergeInfo(info);
        if (info != null) {
            info.info.putAttribute("writer_generation", String.valueOf(writerGeneration));
        }
    }
}
