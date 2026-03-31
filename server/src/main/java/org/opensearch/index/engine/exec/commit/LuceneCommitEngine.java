/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.commit;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.misc.store.HardlinkCopyDirectoryWrapper;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.lucene.uid.VersionsAndSeqNoResolver;
import org.opensearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndVersion;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.engine.CombinedDeletionPolicy;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.IndexVersionValue;
import org.opensearch.index.engine.OpenSearchReaderManager;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.index.engine.SoftDeletesPolicy;
import org.opensearch.index.engine.VersionValue;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.Segment;
import org.opensearch.index.engine.exec.lucene.LuceneDataFormat;
import org.opensearch.index.engine.exec.lucene.writer.LuceneWriter;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.TranslogDeletionPolicy;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Base64;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

public class LuceneCommitEngine implements Closeable {

    private final Logger logger;
    private IndexWriter indexWriter;
    private final CombinedDeletionPolicy combinedDeletionPolicy;
    private final Store store;
    private volatile SegmentInfos lastCommittedSegmentInfos;
    private final OpenSearchReaderManager readerManager;
    // Staged delete entries from child writers, applied after addIndexes in addLuceneIndexes.
    private final List<LuceneWriter.DeleteEntry> pendingDeletes = new ArrayList<>();

    public LuceneCommitEngine(
        Store store,
        CombinedDeletionPolicy combinedDeletionPolicy,
        IndexWriter indexWriter)
        throws IOException {
        this.logger = Loggers.getLogger(LuceneCommitEngine.class, store.shardId());
        this.combinedDeletionPolicy = combinedDeletionPolicy;
        this.store = store;
        this.indexWriter = indexWriter;
        this.lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
        if (indexWriter != null) {
            OpenSearchDirectoryReader reader = OpenSearchDirectoryReader.wrap(
                DirectoryReader.open(indexWriter), store.shardId()
            );
            this.readerManager = new OpenSearchReaderManager(reader);
        } else {
            this.readerManager = null;
        }
    }

    public synchronized void addLuceneIndexes(List<Segment> segments) throws IOException {

        for(Segment segment : segments) {
            WriterFileSet wfs = segment.getDFGroupedSearchableFiles().get(LuceneDataFormat.LUCENE.name());
            if(wfs == null || wfs.refresh()) continue;

            try {
                Path writerDir = Path.of(indexWriter.getDirectory().toString()).toAbsolutePath().normalize();
                Path segmentDir = Path.of(wfs.getDirectory()).toAbsolutePath().normalize();
                if (!writerDir.equals(segmentDir)) {
                    NIOFSDirectory segDir = new NIOFSDirectory(segmentDir);
                    try {
                        indexWriter.addIndexes(new HardlinkCopyDirectoryWrapper(segDir));
                    } finally {
                        segDir.close();
                    }
                }
                wfs.setRefreshed();
            } catch (IOException e) {
                throw new RuntimeException("Not able to copy it to the main writer in commiter: {}", e);
            }
        }

        // Apply staged cross-writer deletes as a single batch to avoid repeated
        // processEvents/applyQueryDeletes passes inside IndexWriter (O(n^2) when called per-entry).
        if (!pendingDeletes.isEmpty()) {
            logger.info("[COMMIT_DEBUG] Applying {} staged deletes after addIndexes", pendingDeletes.size());
            Query[] deleteQueries = new Query[pendingDeletes.size()];
            for (int i = 0; i < pendingDeletes.size(); i++) {
                LuceneWriter.DeleteEntry entry = pendingDeletes.get(i);
                deleteQueries[i] = new BooleanQuery.Builder()
                    .add(new TermQuery(entry.getTerm()), BooleanClause.Occur.MUST)
                    .add(NumericDocValuesField.newSlowRangeQuery(
                        SeqNoFieldMapper.NAME, Long.MIN_VALUE, entry.getSeqNo() - 1), BooleanClause.Occur.MUST)
                    .build();
            }
            indexWriter.deleteDocuments(deleteQueries);
            pendingDeletes.clear();
        }

        final Map<Long, Segment> segmentByGeneration =
            segments.stream().collect(Collectors.toMap(Segment::getGeneration, Function.identity()));

        try (DirectoryReader dr = DirectoryReader.open(indexWriter)){
            for(LeafReaderContext leaf : dr.getContext().leaves()) {
                SegmentCommitInfo segmentCommitInfo = Lucene.segmentReader(leaf.reader()).getSegmentInfo();
                String generationAttr = segmentCommitInfo.info.getAttribute("writer_generation");
                if(generationAttr == null) {
                    throw new RuntimeException("failed to fetch writer generation");
                }
                long writerGeneration = Long.parseLong(generationAttr);
                if (segmentByGeneration.containsKey(writerGeneration)) {
                    WriterFileSet writerFileSet =
                        segmentByGeneration.get(writerGeneration).getDFGroupedSearchableFiles().get(DataFormat.LUCENE.name());
                    Path oldDirectoryPath = Path.of(writerFileSet.getDirectory());
                    segmentByGeneration.get(writerGeneration).addSearchableFiles(
                        DataFormat.LUCENE.name(),
                        writerFileSet.withDirectoryAndFiles(indexWriter.getDirectory().toString(), new HashSet<>(segmentCommitInfo.files()))
                    );
                    // Deletes the older path once the file path has been updated
                    IOUtils.rm(oldDirectoryPath);
                }
            }
        }
        readerManager.maybeRefresh();
    }

    public synchronized void stageDeletes(List<LuceneWriter.DeleteEntry> entries) {
        pendingDeletes.addAll(entries);
    }

    public synchronized void deleteDocuments(List<Term> terms) throws IOException {
        if (!terms.isEmpty()) {
            logger.info("[COMMIT_DEBUG] Deleting {} docs from parent writer, terms={}", terms.size(), terms);
            indexWriter.deleteDocuments(terms.toArray(new Term[0]));
            readerManager.maybeRefresh();
        }
    }

    public OpenSearchDirectoryReader acquireReader() throws IOException {
        return readerManager.acquire();
    }

    public void releaseReader(OpenSearchDirectoryReader reader) throws IOException {
        readerManager.release(reader);
    }

    public void logDocCount(String context) throws IOException {
        final OpenSearchDirectoryReader reader = readerManager.acquire();
        try {
            logger.info("[COMMIT_DEBUG] {} numDocs={}, maxDoc={}, deletedDocs={}",
                context, reader.numDocs(), reader.maxDoc(), reader.maxDoc() - reader.numDocs());
        } finally {
            readerManager.release(reader);
        }
    }

    public VersionValue resolveDocVersionFromIndex(Term uid, boolean loadSeqNo) throws IOException {
        if (readerManager == null) {
            return null;
        }
        final OpenSearchDirectoryReader reader = readerManager.acquire();
        try {
            DocIdAndVersion docIdAndVersion = VersionsAndSeqNoResolver.loadDocIdAndVersion(reader, uid, loadSeqNo);
            if (docIdAndVersion != null) {
                return new IndexVersionValue(null, docIdAndVersion.version, docIdAndVersion.seqNo, docIdAndVersion.primaryTerm);
            }
            return null;
        } finally {
            readerManager.release(reader);
        }
    }

    public synchronized CommitPoint commit(Iterable<Map.Entry<String, String>> commitData, CatalogSnapshot catalogSnapshot) {
        indexWriter.setLiveCommitData(commitData);
        try {
            indexWriter.commit();
            IndexCommit indexCommit = combinedDeletionPolicy.getLastCommit();
            refreshLastCommittedSegmentInfos();
            return CommitPoint.builder()
                .commitFileName(indexCommit.getSegmentsFileName())
                .fileNames(indexCommit.getFileNames())
                .commitData(indexCommit.getUserData())
                .generation(indexCommit.getGeneration())
                .directory(Path.of(indexCommit.getSegmentsFileName()).getParent())
                .build();
        } catch (IOException e) {
            throw new RuntimeException("lucene commit engine failed", e);
        }
    }

    private void refreshLastCommittedSegmentInfos() {
        store.incRef();
        try {
            lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
        } catch (Exception e) {
            throw new RuntimeException("failed to read latest segment infos on commit", e);
        } finally {
            store.decRef();
        }
    }

    public Map<String, String> getLastCommittedData() {
        return MapBuilder.<String, String>newMapBuilder().putAll(lastCommittedSegmentInfos.getUserData()).immutableMap();
    }

    public CommitStats getCommitStats() {
        String segmentId = Base64.getEncoder().encodeToString(lastCommittedSegmentInfos.getId());
        // TODO: Implement numDocs
        return new CommitStats(lastCommittedSegmentInfos.getUserData(), lastCommittedSegmentInfos.getLastGeneration(), segmentId, 0);
    }

    public SafeCommitInfo getSafeCommitInfo() {
        return this.combinedDeletionPolicy.getSafeCommitInfo();
    }

    /**
     * Sets the soft deletes policy on the underlying CombinedDeletionPolicy.
     * This enables proper checkpoint tracking for peer recovery and translog trimming.
     * Must be called after the SoftDeletesPolicy is created.
     */
    public void setSoftDeletesPolicy(SoftDeletesPolicy softDeletesPolicy) {
        this.combinedDeletionPolicy.setSoftDeletesPolicy(softDeletesPolicy);
    }

    /**
     * Acquires the most recent safe index commit snapshot.
     * All index files referenced by this commit won't be freed until the commit/snapshot is closed.
     * This method is required for replica recovery operations.
     */
    public GatedCloseable<IndexCommit> acquireSafeIndexCommit() throws EngineException {
        try {
            // Use CombinedDeletionPolicy to acquire safe commit
            IndexCommit safeCommit = combinedDeletionPolicy.acquireIndexCommit(true);
            return new GatedCloseable<>(safeCommit, () -> {
                try {
                    combinedDeletionPolicy.releaseCommit(safeCommit);
                } catch (Exception e) {
                    logger.warn("Failed to release safe commit", e);
                }
            });
        } catch (Exception e) {
            throw new EngineException(store.shardId(), "Failed to acquire safe index commit", e);
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(readerManager, indexWriter);
    }
}
