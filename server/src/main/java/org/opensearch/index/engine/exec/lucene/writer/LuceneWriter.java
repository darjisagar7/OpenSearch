/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.writer;

import org.apache.lucene.index.IndexWriter;
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

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LuceneWriter implements Writer<LuceneDocumentInput> {

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
            long seqNum = writer.addDocument(documentInput.getDocument());
            return new WriteResult(true, null, 1, 1, seqNum);
        } catch (IOException exception) {
            return new WriteResult(false, exception, 1, 1, 1);
        }
    }

    @Override
    public WriteResult updateDocumentToWriter(Term uid, LuceneDocumentInput documentInput) {
        try {
            long seqNum = writer.updateDocument(uid, documentInput.getDocument());
            lastDeleteEntrySet.put(uid.bytes(), new DeleteEntry(uid));
            return new WriteResult(true, null, 1, 1, seqNum);
        } catch (IOException exception) {
            return new WriteResult(false, exception, 1, 1, 1);
        }
    }

    @Override
    public void deleteDocumentFromWriter(Term uid) throws IOException {
        writer.deleteDocuments(uid);
        lastDeleteEntrySet.put(uid.bytes(), new DeleteEntry(uid));
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

    public List<Term> getDeleteTerms() {
        List<Term> terms = new ArrayList<>();
        for (DeleteEntry entry : lastDeleteEntrySet.values()) {
            terms.add(entry.getTerm());
        }
        return terms;
    }

    public static class DeleteEntry {
        private final Term term;

        public DeleteEntry(Term term) {
            this.term = term;
        }

        public Term getTerm() {
            return term;
        }
    }
}
