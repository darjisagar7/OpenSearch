/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Compact representation of row ID mappings using PackedLongValues + offsets.
 *
 * Structure:
 * - Single flat array: mapping[position] = newRowId
 * - fileOffsets maps fileId -> starting offset in the array
 * - fileSizes maps fileId -> number of rows in that file
 *
 * Offsets are assigned in the order files are processed by Rust (input_files order),
 * NOT sorted. This ensures the mapping is independent of file ordering.
 *
 * Example: Rust processes files in order [file_5, file_0, file_3]
 * - file_5 (2 rows): offset=0, mapping[0]=2, mapping[1]=3
 * - file_0 (3 rows): offset=2, mapping[2]=0, mapping[3]=4, mapping[4]=1
 * - file_3 (1 row):  offset=5, mapping[5]=5
 *
 * Lookup: newRowId = mapping.get(fileOffsets.get(fileId) + oldRowId)
 */
public final class RowIdMapping {

    private final PackedLongValues mapping;
    private final Map<String, Integer> fileOffsets;
    private final Map<String, Integer> fileSizes;
    private final String outputFileId;

    /**
     * Creates a RowIdMapping from a mapping array, file offsets, and file sizes.
     *
     * @param mappingArray array where index=position, value=newRowId
     * @param fileOffsets map of fileId to starting offset in the mapping array
     * @param fileSizes map of fileId to number of rows in that file
     * @param outputFileId the output file ID
     */
    public RowIdMapping(long[] mappingArray, Map<String, Integer> fileOffsets,
                        Map<String, Integer> fileSizes, String outputFileId) {
        Objects.requireNonNull(mappingArray, "mappingArray cannot be null");
        Objects.requireNonNull(fileOffsets, "fileOffsets cannot be null");
        Objects.requireNonNull(fileSizes, "fileSizes cannot be null");
        Objects.requireNonNull(outputFileId, "outputFileId cannot be null");

        PackedLongValues.Builder builder = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
        for (long value : mappingArray) {
            builder.add(value);
        }

        this.mapping = builder.build();
        this.fileOffsets = Collections.unmodifiableMap(new HashMap<>(fileOffsets));
        this.fileSizes = Collections.unmodifiableMap(new HashMap<>(fileSizes));
        this.outputFileId = outputFileId;
    }

    /**
     * Looks up the new row ID for a given old row ID and file ID.
     * O(1) lookup.
     */
    public long getNewRowId(long oldRowId, String fileId) {
        Integer offset = fileOffsets.get(fileId);
        if (offset == null) {
            return -1L;
        }
        Integer size = fileSizes.get(fileId);
        if (size == null || oldRowId < 0 || oldRowId >= size) {
            return -1L;
        }
        return mapping.get(offset + (int) oldRowId);
    }

    /**
     * Gets the number of rows for a specific file.
     */
    public int getFileSize(String fileId) {
        Integer size = fileSizes.get(fileId);
        return size != null ? size : 0;
    }

    /**
     * Gets the new row ID at an absolute position in the mapping array.
     */
    public long getNewRowIdAt(int position) {
        return mapping.get(position);
    }

    public String getFileId() {
        return outputFileId;
    }

    public Map<String, Integer> getFileOffsets() {
        return fileOffsets;
    }

    public Map<String, Integer> getFileSizes() {
        return fileSizes;
    }

    public int size() {
        return (int) mapping.size();
    }

    @Override
    public String toString() {
        return "RowIdMapping{" +
            "size=" + mapping.size() +
            ", files=" + fileOffsets.size() +
            ", outputFileId='" + outputFileId + '\'' +
            ", estimatedMemoryBytes=" + mapping.ramBytesUsed() +
            '}';
    }
}
