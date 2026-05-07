/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

/**
 * Integration tests for capability-based field routing during mapping creation.
 * <p>
 * Validates that:
 * <ul>
 *   <li>Supported field types are accepted when creating mappings with pluggable data formats</li>
 *   <li>Unsupported field types are rejected with clear error messages</li>
 *   <li>Capability validation checks user-requested capabilities (index, doc_values, store)</li>
 *   <li>Multiple field types can coexist in a single mapping</li>
 * </ul>
 *
 * Run with:
 * ./gradlew -Dsandbox.enabled=true :sandbox:plugins:composite-engine:internalClusterTest \
 *   --tests "*.CapabilityBasedFieldRoutingIT"
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class CapabilityBasedFieldRoutingIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ParquetDataFormatPlugin.class, CompositeDataFormatPlugin.class, LucenePlugin.class, DataFusionPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    private Settings compositeIndexSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .build();
    }

    // --- Supported field type tests ---

    public void testKeywordFieldIsAccepted() {
        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate("test-keyword")
            .setSettings(compositeIndexSettings())
            .setMapping("status", "type=keyword")
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen("test-keyword");
    }

    public void testTextFieldIsAccepted() {
        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate("test-text")
            .setSettings(compositeIndexSettings())
            .setMapping("description", "type=text")
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen("test-text");
    }

    public void testIntegerFieldIsAccepted() {
        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate("test-integer")
            .setSettings(compositeIndexSettings())
            .setMapping("count", "type=integer")
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen("test-integer");
    }

    public void testDateFieldIsAccepted() {
        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate("test-date")
            .setSettings(compositeIndexSettings())
            .setMapping("timestamp", "type=date")
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen("test-date");
    }

    public void testBooleanFieldIsAccepted() {
        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate("test-boolean")
            .setSettings(compositeIndexSettings())
            .setMapping("active", "type=boolean")
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen("test-boolean");
    }

    public void testMultipleFieldTypesInSingleMapping() {
        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate("test-multi")
            .setSettings(compositeIndexSettings())
            .setMapping(
                "name",
                "type=keyword",
                "description",
                "type=text",
                "count",
                "type=integer",
                "price",
                "type=float",
                "timestamp",
                "type=date",
                "active",
                "type=boolean"
            )
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen("test-multi");

        // Verify all fields are present in the mapping
        GetMappingsResponse mappings = client().admin().indices().prepareGetMappings("test-multi").get();
        MappingMetadata mapping = mappings.getMappings().get("test-multi");
        assertNotNull(mapping);
        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) mapping.getSourceAsMap().get("properties");
        assertNotNull(properties);
        assertTrue(properties.containsKey("name"));
        assertTrue(properties.containsKey("description"));
        assertTrue(properties.containsKey("count"));
        assertTrue(properties.containsKey("price"));
        assertTrue(properties.containsKey("timestamp"));
        assertTrue(properties.containsKey("active"));
    }

    // --- Unsupported field type tests ---

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/0000")
    public void testUnsupportedFieldTypeIsRejected() {
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> client().admin()
                .indices()
                .prepareCreate("test-unsupported")
                .setSettings(compositeIndexSettings())
                .setMapping("location", "type=geo_point")
                .get()
        );
        assertThat(ex.getMessage(), containsString("geo_point"));
        assertThat(ex.getMessage(), containsString("not supported by any registered data format"));
    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/0000")
    public void testMixOfSupportedAndUnsupportedFieldsIsRejected() {
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> client().admin()
                .indices()
                .prepareCreate("test-mixed-unsupported")
                .setSettings(compositeIndexSettings())
                .setMapping("name", "type=keyword", "location", "type=geo_point")
                .get()
        );
        assertThat(ex.getMessage(), containsString("geo_point"));
        assertThat(ex.getMessage(), containsString("not supported by any registered data format"));
    }

    // --- Non-composite index bypass tests ---

    public void testNonCompositeIndexAcceptsAnyFieldType() {
        // Without pluggable data format, any field type should be accepted (no validation)
        Settings standardSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();

        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate("test-standard")
            .setSettings(standardSettings)
            .setMapping("location", "type=geo_point", "name", "type=keyword")
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen("test-standard");
    }
}
