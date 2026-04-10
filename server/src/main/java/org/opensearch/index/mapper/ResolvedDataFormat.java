/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;

import java.util.Map;
import java.util.Set;

/**
 * Holds a resolved data format name and its pre-indexed capability map (field type name → capabilities).
 * Used during mapping parsing to validate field compatibility with configured data formats.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public record ResolvedDataFormat(String formatName, Map<String, Set<FieldTypeCapabilities.Capability>> capabilitiesByFieldType) {
}
