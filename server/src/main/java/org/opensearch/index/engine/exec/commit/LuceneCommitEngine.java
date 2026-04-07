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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
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
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.TranslogDeletionPolicy;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Base64;
import java.util.ArrayList;
import java.util.HashMap;
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

    /**
     * Merges child writer segments into the parent (commit) IndexWriter.
     *
     * Cross-writer deletes are applied in two phases using lightweight term deletes:
     *
     * Phase 1 (before addIndexes): For document updates, delete the old version from the parent
     *   writer only if the parent holds a version older than the incoming update. This is safe
     *   because the new version hasn't been copied in yet, so a term delete on _id only hits
     *   the stale copy. A version check prevents deleting a newer version from a prior cycle.
     *
     * Phase 2 (after addIndexes): For explicit deletes, remove all versions of the _id including
     *   documents just copied from other child writers in this batch.
     *
     * Cross-writer update duplicates (e.g., writer A has v10 and writer B has v12 for the same _id)
     * are resolved at read time by highest seqNo. Stale copies are cleaned up by segment merges.
     */
    public synchronized void addLuceneIndexes(List<Segment> segments) throws IOException {
        long t0 = System.nanoTime();
        List<Term> pendingExplicitDeletes = processStagedDeletes();
        long t1 = System.nanoTime();
        copyChildSegmentsToParent(segments);
        long t2 = System.nanoTime();
        deleteExplicitlyDeletedDocs(pendingExplicitDeletes);
        long t3 = System.nanoTime();
        readerManager.maybeRefresh();
        long t4 = System.nanoTime();
        logger.info("[REFRESH_TIMING] addLuceneIndexes total={}ms staged={}ms copy={}ms deletes={}ms refresh={}ms segments={}",
            (t4 - t0) / 1_000_000, (t1 - t0) / 1_000_000, (t2 - t1) / 1_000_000,
            (t3 - t2) / 1_000_000, (t4 - t3) / 1_000_000, segments.size());
    }

    /**
     * Consumes {@link #pendingDeletes}, deduplicates by _id (keeping the highest seqNo entry),
     * and splits them into:
     *   - Update entries: old parent versions deleted immediately via term delete (version-checked).
     *   - Explicit delete entries: returned for post-addIndexes application.
     */
    private List<Term> processStagedDeletes() throws IOException {
        if (pendingDeletes.isEmpty()) {
            return List.of();
        }

        Map<BytesRef, LuceneWriter.DeleteEntry> entriesByDocId = deduplicateByDocId(pendingDeletes);
        logger.trace("[COMMIT_DEBUG] Staged deletes: {} total, {} unique ids",
            pendingDeletes.size(), entriesByDocId.size());

        List<Term> explicitDeleteTerms = new ArrayList<>();
        List<LuceneWriter.DeleteEntry> updateDeleteEntries = new ArrayList<>();

        for (LuceneWriter.DeleteEntry entry : entriesByDocId.values()) {
            if (entry.getSeqNo() == Long.MAX_VALUE) {
                explicitDeleteTerms.add(entry.getTerm());
            } else {
                updateDeleteEntries.add(entry);
            }
        }

        List<Term> staleParentTerms = findStaleParentVersions(updateDeleteEntries);
        if (!staleParentTerms.isEmpty()) {
            logger.trace("[COMMIT_DEBUG] Deleting {} stale parent versions before addIndexes", staleParentTerms.size());
            indexWriter.deleteDocuments(staleParentTerms.toArray(new Term[0]));
        }

        pendingDeletes.clear();
        return explicitDeleteTerms;
    }

    /**
     * Collapses multiple delete entries for the same _id into one, keeping the entry
     * with the highest seqNo. This handles the case where multiple child writers
     * produce delete entries for the same document in a single flush cycle.
     */
    private Map<BytesRef, LuceneWriter.DeleteEntry> deduplicateByDocId(
        List<LuceneWriter.DeleteEntry> deleteEntries) {
        Map<BytesRef, LuceneWriter.DeleteEntry> entriesByDocId = new HashMap<>();
        for (LuceneWriter.DeleteEntry entry : deleteEntries) {
            entriesByDocId.merge(entry.getTerm().bytes(), entry,
                (existing, incoming) -> incoming.getSeqNo() >= existing.getSeqNo() ? incoming : existing);
        }
        return entriesByDocId;
    }

    /**
     * For each update entry, checks whether the parent writer holds an older version of
     * the document. Returns the _id terms for documents that need to be deleted from the
     * parent before new segments are added.
     */
    private List<Term> findStaleParentVersions(
        List<LuceneWriter.DeleteEntry> updateDeleteEntries) throws IOException {
        List<Term> staleTerms = new ArrayList<>();
        if (updateDeleteEntries.isEmpty() || readerManager == null) {
            return staleTerms;
        }
        final OpenSearchDirectoryReader reader = readerManager.acquire();
        try {
            for (LuceneWriter.DeleteEntry entry : updateDeleteEntries) {
                DocIdAndVersion parentVersion =
                    VersionsAndSeqNoResolver.loadDocIdAndVersion(reader, entry.getTerm(), true);
                if (parentVersion != null && parentVersion.seqNo < entry.getSeqNo()) {
                    staleTerms.add(entry.getTerm());
                }
            }
        } finally {
            readerManager.release(reader);
        }
        return staleTerms;
    }

    private void copyChildSegmentsToParent(List<Segment> segments) throws IOException {
        for (Segment segment : segments) {
            WriterFileSet luceneFiles = segment.getDFGroupedSearchableFiles().get(LuceneDataFormat.LUCENE.name());
            if (luceneFiles == null || luceneFiles.refresh()) continue;

            Path parentDir = Path.of(indexWriter.getDirectory().toString()).toAbsolutePath().normalize();
            Path childDir = Path.of(luceneFiles.getDirectory()).toAbsolutePath().normalize();
            if (parentDir.equals(childDir)) {
                luceneFiles.setRefreshed();
                continue;
            }

            NIOFSDirectory childDirectory = new NIOFSDirectory(childDir);
            try {
                indexWriter.addIndexes(childDirectory);
            } finally {
                childDirectory.close();
            }
            luceneFiles.setRefreshed();
        }
    }

    private void deleteExplicitlyDeletedDocs(List<Term> explicitDeleteTerms) throws IOException {
        if (!explicitDeleteTerms.isEmpty()) {
            logger.trace("[COMMIT_DEBUG] Deleting {} explicitly deleted docs after addIndexes",
                explicitDeleteTerms.size());
            indexWriter.deleteDocuments(explicitDeleteTerms.toArray(new Term[0]));
        }
    }

    /**
     * After addIndexes, the copied segments live under the parent writer's directory with
     * new file names. This method updates each Segment's WriterFileSet to reflect the
     * new paths and removes the old child writer directories.
     */
    private void remapSegmentFilePaths(List<Segment> segments) throws IOException {
        final Map<Long, Segment> segmentsByGeneration =
            segments.stream().collect(Collectors.toMap(Segment::getGeneration, Function.identity()));

        try (DirectoryReader reader = DirectoryReader.open(indexWriter)) {
            for (LeafReaderContext leafContext : reader.getContext().leaves()) {
                SegmentCommitInfo commitInfo = Lucene.segmentReader(leafContext.reader()).getSegmentInfo();
                String generationAttr = commitInfo.info.getAttribute("writer_generation");
                if (generationAttr == null) {
                    // Segments without writer_generation are either pre-existing parent segments
                    // or merged segments produced by background merges — skip them.
                    continue;
                }
                long generation = Long.parseLong(generationAttr);
                Segment matchingSegment = segmentsByGeneration.get(generation);
                if (matchingSegment != null) {
                    WriterFileSet oldFileSet =
                        matchingSegment.getDFGroupedSearchableFiles().get(DataFormat.LUCENE.name());
                    Path oldDirectory = Path.of(oldFileSet.getDirectory());
                    matchingSegment.addSearchableFiles(
                        DataFormat.LUCENE.name(),
                        oldFileSet.withDirectoryAndFiles(
                            indexWriter.getDirectory().toString(), new HashSet<>(commitInfo.files()))
                    );
                    IOUtils.rm(oldDirectory);
                }
            }
        }
    }

    public synchronized void stageDeletes(List<LuceneWriter.DeleteEntry> entries) {
        pendingDeletes.addAll(entries);
    }

    public synchronized void deleteDocuments(List<Term> terms) throws IOException {
        if (!terms.isEmpty()) {
            logger.trace("[COMMIT_DEBUG] Deleting {} docs from parent writer, terms={}", terms.size(), terms);
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
            logger.trace("[COMMIT_DEBUG] {} numDocs={}, maxDoc={}, deletedDocs={}",
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
