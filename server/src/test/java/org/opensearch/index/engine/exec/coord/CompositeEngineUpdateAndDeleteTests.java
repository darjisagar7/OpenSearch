/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.junit.After;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.MapperTestUtils;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.exec.composite.CompositeDataFormatWriter;
import org.opensearch.index.engine.exec.lucene.LuceneDataSourcePlugin;
import org.opensearch.index.engine.exec.lucene.engine.LuceneExecutionEngine;
import org.opensearch.index.mapper.*;
import org.opensearch.index.seqno.LocalCheckpointTracker;
import org.opensearch.index.seqno.ReplicationTracker;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.index.translog.listener.TranslogEventListener;
import org.opensearch.plugins.DataSourcePlugin;
import org.opensearch.plugins.PluginsService;
import org.opensearch.plugins.SearchEnginePlugin;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.IntUnaryOperator;

import static java.util.Collections.emptyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CompositeEngineUpdateAndDeleteTests extends OpenSearchTestCase {

    private Store store;
    private ThreadPool threadPool;
    private Path translogDir;
    private CompositeEngine compositeEngine;
    private final ShardId shardId = new ShardId(new Index("test-index", UUID.randomUUID().toString()), 0);
    private IndexSettings indexSettings;
    private MapperService mapperService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());

        Settings settings = Settings.builder()
            .put(IndexSettings.OPTIMIZED_INDEX_ENABLED_SETTING.getKey(), true)
            .put(IndexSettings.INDEX_COMPOSITE_PRIMARY_DATA_FORMAT_SETTING.getKey(), "Lucene")
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.version.created", Version.CURRENT)
            .build();

        indexSettings = IndexSettingsModule.newIndexSettings(shardId.getIndex(), settings);

        Path dataPath = createTempDir().resolve(shardId.getIndex().getUUID()).resolve(String.valueOf(shardId.id()));
        ShardPath shardPath = new ShardPath(false, dataPath, dataPath, shardId);

        Directory directory = newDirectory();
        store = new Store(
            shardId,
            indexSettings,
            directory,
            new DummyShardLock(shardId),
            Store.OnClose.EMPTY,
            shardPath
        );

        // Create initial empty commit with translog UUID
        translogDir = createTempDir("translog");
        String translogUUID = Translog.createEmptyTranslog(
            translogDir,
            SequenceNumbers.NO_OPS_PERFORMED,
            shardId,
            1L
        );
        store.createEmpty(Version.CURRENT.luceneVersion, translogUUID);

        // Create MapperService with keyword + integer fields
        mapperService = MapperTestUtils.newMapperService(
            new NamedXContentRegistry(ClusterModule.getNamedXWriteables()),
            createTempDir(),
            Settings.EMPTY,
            "test-index"
        );
        String mapping = "{\"properties\": {"
            + "\"name\": {\"type\": \"keyword\"},"
            + "\"age\": {\"type\": \"integer\"}"
            + "}}";
        mapperService.merge("_doc", new org.opensearch.common.compress.CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);

        // Mock PluginsService to return LuceneDataSourcePlugin
        PluginsService pluginsService = mock(PluginsService.class);
        LuceneDataSourcePlugin lucenePlugin = new LuceneDataSourcePlugin();
        when(pluginsService.filterPlugins(DataSourcePlugin.class)).thenReturn(List.of(lucenePlugin));
        when(pluginsService.filterPlugins(SearchEnginePlugin.class)).thenReturn(emptyList());

        EngineConfig engineConfig = createEngineConfig();

        compositeEngine = new CompositeEngine(
            engineConfig,
            mapperService,
            pluginsService,
            indexSettings,
            shardPath,
            LocalCheckpointTracker::new,
            TranslogEventListener.NOOP_TRANSLOG_EVENT_LISTENER
        );
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        try {
            if (compositeEngine != null) {
                compositeEngine.close();
            }
        } finally {
            try {
                if (store != null) {
                    store.close();
                }
            } finally {
                if (threadPool != null) {
                    terminate(threadPool);
                }
            }
        }
    }

    private record TestDoc(String id, String name, int age) {
    }

    private static TestDoc doc(String id, String name, int age) {
        return new TestDoc(id, name, age);
    }

    private static TestDoc[] docs(int from, int to, String namePrefix, IntUnaryOperator ageFn) {
        TestDoc[] result = new TestDoc[to - from + 1];
        for (int i = from; i <= to; i++) {
            result[i - from] = doc("doc" + i, namePrefix + i, ageFn.applyAsInt(i));
        }
        return result;
    }

    // --- Tests ---

    public void testIndexAndVerifyDocumentExists() throws Exception {
        TestDoc alice = doc("1", "Alice", 30);
        indexDoc(alice);
        refresh();
        assertDocCount(1);
        assertFieldValues(alice);
    }

    public void testIndexMultipleDocsAndVerifyCount() throws Exception {
        indexDocs(doc("1", "Alice", 30), doc("2", "Bob", 25), doc("3", "Charlie", 35));
        refresh();
        assertDocCount(3);
    }

    public void testUpdateDocumentVerifyNewFieldValue() throws Exception {
        TestDoc original = doc("1", "Alice", 30);
        TestDoc updated = doc("1", "Alice_Updated", 31);

        indexDoc(original);
        refresh();
        assertFieldValues(original);

        indexDoc(updated);
        refresh();
        assertDocCount(1);
        assertFieldValues(updated);

        // Verify seqNo was incremented (update gets seqNo=1, original was seqNo=0)
        assertSeqNo("1", 1L);
    }

    public void testUpdateDocumentVersionIncremented() throws Exception {
        Engine.IndexResult result1 = indexDoc(doc("1", "Alice", 30));
        refresh();

        Engine.IndexResult result2 = indexDoc(doc("1", "Alice_v2", 31));
        assertTrue("Version should increment on update", result2.getVersion() > result1.getVersion());

        refresh();
    }

    public void testDeleteDocumentInOlderGeneration() throws Exception {
        indexDoc(doc("1", "Alice", 30));
        refresh();
        assertDocCount(1);

        // Delete after refresh (doc is now in parent writer / older generation)
        deleteDoc("1");
        refresh();
        assertDocCount(0);
    }

    public void testDeleteNonExistentDocIsNoOp() throws Exception {
        TestDoc alice = doc("1", "Alice", 30);
        indexDoc(alice);
        refresh();

        deleteDoc("non-existent");
        refresh();
        assertDocCount(1);
        assertFieldValues(alice);
    }

    public void testMultipleUpdatesOnSameDocument() throws Exception {
        indexDoc(doc("1", "Alice", 30));
        indexDoc(doc("1", "Alice_v2", 31));
        TestDoc intermediate = doc("1", "Alice_v3", 32);
        indexDoc(intermediate);
        refresh();

        assertDocCount(1);
        assertFieldValues(intermediate);

    }

    public void testUpdateThenDelete() throws Exception {
        indexDoc(doc("1", "Alice", 30));
        refresh();

        indexDoc(doc("1", "Alice_v2", 31));
        refresh();
        assertDocCount(1);

        deleteDoc("1");
        refresh();
        assertDocCount(0);
    }

    public void testMixedOperationsOnMultipleDocs() throws Exception {
        TestDoc charlie = doc("3", "Charlie", 35);
        indexDocs(doc("1", "Alice", 30), doc("2", "Bob", 25), charlie);
        refresh();
        assertDocCount(3);

        TestDoc aliceUpdated = doc("1", "Alice_v2", 31);
        indexDoc(aliceUpdated);
        deleteDoc("2");
        refresh();

        assertDocCount(2);
        assertFieldValues(aliceUpdated, charlie);
    }

    public void testBulkIndexAcrossWritersThenUpdateSubset() throws Exception {
        TestDoc[] originals = docs(1, 20, "original_", i -> i);
        indexDocs(originals);
        refresh();
        assertDocCount(20);
        assertFieldValues(originals);

        // Update first 10 — land in new writer generations
        TestDoc[] updates = docs(1, 10, "updated_", i -> i * 100);
        indexDocs(updates);
        refresh();

        assertDocCount(20);
        // Updated docs have new values
        assertFieldValues(updates);
        // Non-updated docs retain original values
        TestDoc[] unchanged = docs(11, 20, "original_", i -> i);
        assertFieldValues(unchanged);
    }

    public void testUpdatesWithoutIntermediateRefresh() throws Exception {
        // Original and update live in different child writers flushed together.
        TestDoc[] originals = docs(1, 10, "v1_", i -> i);
        TestDoc[] updates = docs(1, 10, "v2_", i -> i + 1000);

        indexDocs(originals);
        indexDocs(updates);
        refresh();

        assertDocCount(10);
        assertFieldValues(updates);
    }

    public void testMultipleUpdateRoundsAcrossGenerations() throws Exception {
        // 3 rounds of updates, each separated by a refresh.
        TestDoc[] round1 = docs(1, 10, "round1_", i -> i);
        TestDoc[] round2 = docs(1, 10, "round2_", i -> i + 100);
        TestDoc[] round3 = docs(1, 10, "round3_", i -> i + 200);

        indexDocs(round1);
        refresh();
        assertDocCount(10);

        indexDocs(round2);
        refresh();
        assertDocCount(10);

        indexDocs(round3);
        refresh();
        assertDocCount(10);

        // Only round 3 values should survive
        assertFieldValues(round3);
    }

    public void testIndexUpdateDelete() throws Exception{
        TestDoc charlie = doc("3", "Charlie", 35);
        TestDoc charlieUpdate_1 = doc("3", "CharlieUpdate_1", 2);
        TestDoc charlieUpdate_2 = doc("3", "CharlieUpdate_2", 3);
        indexDoc(charlie);
        refresh();
        indexDoc(charlieUpdate_1);
        refresh();
        indexDoc(charlieUpdate_2);
        refresh();
        assertFieldValues(charlieUpdate_2);
        deleteDoc(charlie.id);

        assertDocCount(1);
        refresh();
        assertDocCount(0);
    }

    // --- Helpers ---

    private Engine.IndexResult indexDoc(TestDoc doc) throws IOException {
        CompositeDataFormatWriter.CompositeDocumentInput documentInput = compositeEngine.documentInput();
        documentInput.addField(mapperService.fieldType("_id"), Uid.encodeId(doc.id));
        documentInput.addField(mapperService.fieldType("name"), doc.name);
        documentInput.addField(mapperService.fieldType("age"), doc.age);

        ParsedDocument parsedDoc = new ParsedDocument(
            null,
            SeqNoFieldMapper.SequenceIDFields.emptySeqID(),
            doc.id,
            null,
            Collections.emptyList(),
            new BytesArray("{\"name\":\"" + doc.name + "\",\"age\":" + doc.age + "}"),
            org.opensearch.core.xcontent.MediaTypeRegistry.JSON,
            null,
            documentInput
        );

        return compositeEngine.index(new Engine.Index(
            new Term(IdFieldMapper.NAME, Uid.encodeId(doc.id)),
            1L,
            parsedDoc
        ));
    }

    private void indexDocs(TestDoc... docs) throws IOException {
        for (TestDoc doc : docs) {
            indexDoc(doc);
        }
    }

    private void deleteDoc(String id) throws IOException {
        compositeEngine.delete(new Engine.Delete(
            id,
            new Term(IdFieldMapper.NAME, Uid.encodeId(id)),
            1L
        ));
    }

    private void refresh() throws IOException {
        compositeEngine.refresh("test");
    }

    private void assertFieldValues(TestDoc... docs) throws IOException {
        LuceneExecutionEngine luceneEngine = compositeEngine.getLuceneExecutionEngine();
        OpenSearchDirectoryReader reader = luceneEngine.acquireReader();
        try {
            IndexSearcher searcher = new IndexSearcher(reader);
            for (TestDoc doc : docs) {
                TopDocs topDocs = searcher.search(new TermQuery(new Term("_id", Uid.encodeId(doc.id))), 1);
                assertEquals("Document with id=" + doc.id + " should exist", 1, topDocs.totalHits.value());

                int docId = topDocs.scoreDocs[0].doc;
                LeafReaderContext leaf = searcher.getIndexReader().leaves().stream()
                    .filter(l -> docId >= l.docBase && docId < l.docBase + l.reader().maxDoc())
                    .findFirst().orElseThrow();
                int localDocId = docId - leaf.docBase;

                SortedSetDocValues nameDV = leaf.reader().getSortedSetDocValues("name");
                assertTrue("name doc values should exist for doc " + doc.id, nameDV.advanceExact(localDocId));
                assertEquals("name mismatch for doc " + doc.id,
                    doc.name, nameDV.lookupOrd(nameDV.nextOrd()).utf8ToString());

                SortedNumericDocValues ageDV = leaf.reader().getSortedNumericDocValues("age");
                assertTrue("age doc values should exist for doc " + doc.id, ageDV.advanceExact(localDocId));
                assertEquals("age mismatch for doc " + doc.id, doc.age, ageDV.nextValue());
            }
        } finally {
            luceneEngine.releaseReader(reader);
        }
    }

    private void assertSeqNo(String id, long expectedSeqNo) throws IOException {
        LuceneExecutionEngine luceneEngine = compositeEngine.getLuceneExecutionEngine();
        OpenSearchDirectoryReader reader = luceneEngine.acquireReader();
        try {
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs topDocs = searcher.search(new TermQuery(new Term("_id", Uid.encodeId(id))), 1);
            int docId = topDocs.scoreDocs[0].doc;
            LeafReaderContext leaf = searcher.getIndexReader().leaves().stream()
                .filter(l -> docId >= l.docBase && docId < l.docBase + l.reader().maxDoc())
                .findFirst().orElseThrow();
            int localDocId = docId - leaf.docBase;
            NumericDocValues seqNoDV = leaf.reader().getNumericDocValues(SeqNoFieldMapper.NAME);
            assertTrue(seqNoDV.advanceExact(localDocId));
            assertEquals(expectedSeqNo, seqNoDV.longValue());
        } finally {
            luceneEngine.releaseReader(reader);
        }
    }

    private void assertDocCount(int expected) throws IOException {
        LuceneExecutionEngine luceneEngine = compositeEngine.getLuceneExecutionEngine();
        OpenSearchDirectoryReader reader = luceneEngine.acquireReader();
        try {
            assertEquals(expected, reader.numDocs());
        } finally {
            luceneEngine.releaseReader(reader);
        }
    }

    private EngineConfig createEngineConfig() {
        TranslogConfig translogConfig = new TranslogConfig(
            shardId,
            translogDir,
            indexSettings,
            BigArrays.NON_RECYCLING_INSTANCE,
            "",
            false
        );

        ReplicationTracker replicationTracker = new ReplicationTracker(
            shardId,
            org.opensearch.cluster.routing.AllocationId.newInitializing().getId(),
            indexSettings,
            randomNonNegativeLong(),
            SequenceNumbers.NO_OPS_PERFORMED,
            update -> {},
            () -> 0L,
            (leases, listener) -> listener.onResponse(new org.opensearch.action.support.replication.ReplicationResponse()),
            () -> org.opensearch.index.engine.SafeCommitInfo.EMPTY,
            sId -> false
        );

        return new EngineConfig.Builder()
            .shardId(shardId)
            .threadPool(threadPool)
            .indexSettings(indexSettings)
            .warmer(null)
            .store(store)
            .mergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE)
            .analyzer(new org.apache.lucene.analysis.standard.StandardAnalyzer())
            .similarity(new org.apache.lucene.search.similarities.BM25Similarity())
            .codecService(new CodecService(null, indexSettings, logger))
            .eventListener(null)
            .queryCache(IndexSearcher.getDefaultQueryCache())
            .queryCachingPolicy(IndexSearcher.getDefaultQueryCachingPolicy())
            .translogConfig(translogConfig)
            .flushMergesAfter(TimeValue.timeValueMinutes(5))
            .externalRefreshListener(emptyList())
            .internalRefreshListener(emptyList())
            .circuitBreakerService(new NoneCircuitBreakerService())
            .globalCheckpointSupplier(replicationTracker)
            .retentionLeasesSupplier(replicationTracker::getRetentionLeases)
            .primaryTermSupplier(() -> 1L)
            .build();
    }
}
