/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.commit;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.EngineConfig;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Initialization parameters for a {@link Committer}.
 *
 * <p>{@code refreshLock} is the engine-owned lock coordinating post-merge catalog
 * updates with refreshes. Committer-owned writers that participate in merges (e.g.
 * the Lucene {@code MergeIndexWriter}) acquire it inside their commitMerge hook;
 * the engine releases it after applying catalog changes.
 *
 * @param engineConfig engine configuration
 * @param refreshLock  engine-owned refresh lock, transferred through the commit path
 * @opensearch.experimental
 */
@ExperimentalApi
public record CommitterConfig(EngineConfig engineConfig, ReentrantLock refreshLock) {
}
