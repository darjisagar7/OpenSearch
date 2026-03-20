/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

public class CustomOneMerge extends MergePolicy.OneMerge {

    private final RowIdMapping rowIdMapping;

    public CustomOneMerge(List<SegmentCommitInfo> segments, RowIdMapping rowIdMapping) {
        super(segments);
        this.rowIdMapping = rowIdMapping;
    }

    @Override
    public Sorter.DocMap reorder(CodecReader reader, Directory dir, Executor executor) throws IOException {
        int totalDocs = reader.maxDoc();

        // Build oldToNew: maps current doc position -> new position after reorder
        // Build newToOld: maps new position -> original doc position
        int[] oldToNew = new int[totalDocs];
        int[] newToOld = new int[totalDocs];

        // Track doc offset per segment, keyed by writer_generation attribute
        // (matches RowId.fileId which is now the generation number string)
        Map<String, Integer> fileIdToBaseDoc = new HashMap<>();
        int baseDoc = 0;
        for (SegmentCommitInfo segmentInfo : segments) {
            String writerGen = segmentInfo.info.getAttribute("writer_generation");
            if (writerGen != null) {
                fileIdToBaseDoc.put(writerGen, baseDoc);
            }
            baseDoc += segmentInfo.info.maxDoc();
        }

        // Build mapping from RowIdMapping
        // Iterate over each file in the mapping and remap old doc positions to new positions
        for (Map.Entry<String, Integer> offsetEntry : rowIdMapping.getFileOffsets().entrySet()) {
            String fileId = offsetEntry.getKey();
            Integer segmentBase = fileIdToBaseDoc.get(fileId);
            if (segmentBase == null) {
                continue;
            }
            int fileSize = rowIdMapping.getFileSize(fileId);
            for (int oldRowInFile = 0; oldRowInFile < fileSize; oldRowInFile++) {
                long newRowId = rowIdMapping.getNewRowId(oldRowInFile, fileId);
                if (newRowId < 0) {
                    continue;
                }
                int oldDocId = segmentBase + oldRowInFile;
                int newDocId = (int) newRowId;

                if (oldDocId < totalDocs && newDocId < totalDocs) {
                    oldToNew[oldDocId] = newDocId;
                    newToOld[newDocId] = oldDocId;
                }
            }
        }

        return new Sorter.DocMap() {
            @Override
            public int oldToNew(int docID) {
                return oldToNew[docID];
            }

            @Override
            public int newToOld(int docID) {
                return newToOld[docID];
            }

            @Override
            public int size() {
                return totalDocs;
            }
        };
    }
}
