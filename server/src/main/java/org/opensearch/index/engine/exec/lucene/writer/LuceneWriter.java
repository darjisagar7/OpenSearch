/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.writer;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.EngineRole;
import org.opensearch.index.engine.exec.FileInfos;
import org.opensearch.index.engine.exec.FlushIn;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.engine.exec.Writer;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.SeqNoFieldMapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LuceneWriter implements Writer<LuceneDocumentInput> {

    private static final Logger logger = LogManager.getLogger(LuceneWriter.class);
    private final IndexWriter writer;
    private final long writerGeneration;
    private final Path directoryPath;
    private final EngineRole engineRole;
    private final Map<BytesRef, DeleteEntry> lastDeleteEntrySet;

    public LuceneWriter(Path directoryPath, IndexWriter writer, long writerGeneration, EngineRole engineRole) {
        this.directoryPath = directoryPath;
        this.writer = writer;
        this.writerGeneration = writerGeneration;
        this.engineRole = engineRole;
        this.lastDeleteEntrySet = new HashMap<>();
    }

    @Override
    public WriteResult addDoc(LuceneDocumentInput documentInput) throws IOException {
        return addToWriter(documentInput);
    }

    public WriteResult updateDoc(LuceneDocumentInput documentInput, Term uid) throws IOException {
        return updateDocumentToWriter(uid, documentInput);
    }

    @Override
    public WriteResult addToWriter(LuceneDocumentInput documentInput) {
        try {
            logger.info("[LUCENE_WRITER] addDocument gen=[{}] numFields=[{}]",
                writerGeneration, documentInput.getDocument().getFields().size());
            long seqNum = writer.addDocument(documentInput.getDocument());
            logger.info("[LUCENE_WRITER] addDocument complete seqNum=[{}]", seqNum);
            return new WriteResult(true, null, 1, 1, seqNum);
        } catch (IOException exception) {
            return new WriteResult(false, exception, 1, 1, 1);
        }
    }

    @Override
    public WriteResult updateDocumentToWriter(Term uid, LuceneDocumentInput documentInput) {
        try {
            logger.info("[LUCENE_WRITER] updateDocument uid=[{}] gen=[{}] numFields=[{}]",
                uid, writerGeneration, documentInput.getDocument().getFields().size());
            long seqNum = writer.updateDocument(uid, documentInput.getDocument());
            long seqNo = extractSeqNo(documentInput);
            lastDeleteEntrySet.put(uid.bytes(), new DeleteEntry(uid, seqNo));
            logger.info("[LUCENE_WRITER] updateDocument complete seqNum=[{}] seqNo=[{}] deleteEntries=[{}]",
                seqNum, seqNo, lastDeleteEntrySet.size());
            return new WriteResult(true, null, 1, 1, seqNum);
        } catch (IOException exception) {
            return new WriteResult(false, exception, 1, 1, 1);
        }
    }

    @Override
    public void deleteDocumentFromWriter(Term uid) throws IOException {
        logger.info("[LUCENE_WRITER] deleteDocument uid=[{}] gen=[{}]", uid, writerGeneration);
        writer.deleteDocuments(uid);
        lastDeleteEntrySet.put(uid.bytes(), new DeleteEntry(uid, Long.MAX_VALUE));
        logger.info("[LUCENE_WRITER] deleteDocument complete deleteEntries=[{}]", lastDeleteEntrySet.size());
    }

    private long extractSeqNo(LuceneDocumentInput documentInput) {
        IndexableField field = documentInput.getDocument().getField(SeqNoFieldMapper.NAME);
        return field != null && field.numericValue() != null ? field.numericValue().longValue() : -1;
    }

    @Override
    public FileInfos flush(FlushIn flushIn) throws IOException {
        writer.forceMerge(1);
        WriterFileSet.Builder writerFileSetBuilder =
            WriterFileSet.builder().directory(directoryPath).writerGeneration(writerGeneration).addNumRows(writer.getDocStats().numDocs);
        return FileInfos.builder().putWriterFileSet(DataFormat.LUCENE, writerFileSetBuilder.build()).build();
    }

    @Override
    public void sync() throws IOException {

    }

    @Override
    public void close() throws IOException {
        writer.close();
    }

    @Override
    public LuceneDocumentInput newDocumentInput() {
        return new LuceneDocumentInput(new ParseContext.Document(), writer, engineRole);
    }

    public List<DeleteEntry> getDeleteEntries() {
        return new ArrayList<>(lastDeleteEntrySet.values());
    }

    public static class DeleteEntry {
        private final Term term;
        private final long seqNo;

        public DeleteEntry(Term term, long seqNo) {
            this.term = term;
            this.seqNo = seqNo;
        }

        public Term getTerm() {
            return term;
        }

        public long getSeqNo() {
            return seqNo;
        }
    }
}
