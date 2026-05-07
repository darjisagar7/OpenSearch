/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.engine.dataformat.DocumentInput;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Standalone reader that opens the Lucene index created by {@link LuceneMergerDemo}
 * and prints every document with its stored fields and {@code ___row_id} doc values.
 *
 * <p>Usage: Run as a Java main class after running {@link LuceneMergerDemo}.
 */
@SuppressForbidden(reason = "Demo CLI tool uses System.out to print index contents")
public class LuceneMergedIndexReader {

    private static final String ROW_ID_FIELD = DocumentInput.ROW_ID_FIELD;
    private static final Path INDEX_DIR = Path.of("/tmp/lucene-merger-demo");

    public static void main(String[] args) throws IOException {
        System.out.println("=== Lucene Merged Index Reader ===");
        System.out.println("Reading index from: " + INDEX_DIR);
        System.out.println();

        try (Directory directory = NIOFSDirectory.open(INDEX_DIR); DirectoryReader reader = DirectoryReader.open(directory)) {

            System.out.println("Total documents: " + reader.numDocs());
            System.out.println("Total segments:  " + reader.leaves().size());
            System.out.println();

            int segmentIndex = 0;
            for (LeafReaderContext leafCtx : reader.leaves()) {
                int maxDoc = leafCtx.reader().maxDoc();
                String segmentName = "unknown";
                String writerGen = "N/A";

                // Extract segment metadata if available
                if (leafCtx.reader() instanceof SegmentReader segmentReader) {
                    SegmentCommitInfo sci = segmentReader.getSegmentInfo();
                    segmentName = sci.info.name;
                    String genAttr = sci.info.getAttribute("writer_generation");
                    if (genAttr != null) {
                        writerGen = genAttr;
                    }
                }

                System.out.println("--- Segment " + segmentIndex + " ---");
                System.out.println("  Name:              " + segmentName);
                System.out.println("  Writer generation: " + writerGen);
                System.out.println("  Doc count:         " + maxDoc);
                System.out.println();

                // Get the ___row_id doc values iterator for this segment
                SortedNumericDocValues rowIdDV = leafCtx.reader().getSortedNumericDocValues(ROW_ID_FIELD);

                System.out.printf("  %-8s %-12s %-25s %-15s %-10s%n", "DocID", "ID Field", "Data Field", "Category", "__row_id__");
                System.out.printf("  %-8s %-12s %-25s %-15s %-10s%n", "-----", "--------", "----------", "--------", "---------");

                for (int docId = 0; docId < maxDoc; docId++) {
                    // Read stored fields
                    Document doc = leafCtx.reader().storedFields().document(docId);
                    String id = doc.get("id");
                    String data = doc.get("data");
                    String category = doc.get("category");

                    // Read ___row_id doc value
                    long rowId = -1;
                    if (rowIdDV != null && rowIdDV.advanceExact(docId)) {
                        rowId = rowIdDV.nextValue();
                    }

                    System.out.printf(
                        "  %-8d %-12s %-25s %-15s %-10d%n",
                        docId,
                        id != null ? id : "null",
                        data != null ? truncate(data, 23) : "null",
                        category != null ? category : "null",
                        rowId
                    );
                }
                System.out.println();
                segmentIndex++;
            }

            System.out.println("=== Done ===");
        }
    }

    private static String truncate(String s, int maxLen) {
        if (s.length() <= maxLen) {
            return s;
        }
        return s.substring(0, maxLen - 2) + "..";
    }
}
