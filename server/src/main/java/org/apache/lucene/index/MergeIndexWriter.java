/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.index;

import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An {@link IndexWriter} subclass that exposes Lucene's internal {@code merge(OneMerge)}
 * path for use by the pluggable data format merge infrastructure.
 *
 * <p>The internal merge path handles the full segment lifecycle including reference-counted
 * file cleanup via {@code IndexFileDeleter}. If the merge fails, old segments are preserved
 * and the partially-written merged segment is cleaned up — providing a safe rollback mechanism.
 *
 * <p>This class is placed in the {@code org.apache.lucene.index} package to access
 * package-private fields on {@link MergePolicy.OneMerge} required for merge registration.
 *
 * <p>The {@link IndexWriterConfig} used to construct this writer must set a
 * {@link SerialMergeScheduler} to avoid the {@link ConcurrentMergeScheduler} thread
 * assertion in {@code wrapForMerge}, since pluggable data format merges run on the
 * engine's own merge thread pool rather than Lucene's {@code MergeThread}.
 *
 * <p>The engine-owned {@code refreshLock} is acquired by {@link #executeMerge} before
 * invoking Lucene's internal merge, matching the lock acquisition order used by the
 * engine's refresh path ({@code refreshLock} → {@code IndexWriter} monitor). The engine
 * releases the lock after applying catalog changes. On merge failure, the lock is
 * released before propagating the exception.
 *
 * @opensearch.experimental
 */
public class MergeIndexWriter extends IndexWriter {

    private final ReentrantLock refreshLock;

    /**
     * @param refreshLock engine-owned lock acquired inside {@code onMergeComplete}
     *                    on the commit path; the engine is responsible for releasing
     *                    it after applying catalog changes
     */
    public MergeIndexWriter(Directory d, IndexWriterConfig conf, ReentrantLock refreshLock) throws IOException {
        super(d, conf);
        this.refreshLock = Objects.requireNonNull(refreshLock, "refreshLock");
    }

    /**
     * Executes a merge using Lucene's internal merge path which handles:
     * <ol>
     *   <li>mergeInit — creates output segment info, increments file references</li>
     *   <li>mergeMiddle — reads sources via wrapForMerge, applies IndexSort via MultiSorter,
     *       writes merged segment</li>
     *   <li>commitMerge — removes old segments from live list, decrements file references</li>
     *   <li>mergeFinish — cleans up merge tracking state</li>
     * </ol>
     *
     * <p>If the merge fails at any point, old segments are preserved and the partially-written
     * merged segment is cleaned up by IndexFileDeleter's reference counting.
     *
     * <p>Duplicate segment prevention is handled by the caller; this method does not
     * validate against concurrent merges on the same segments.
     *
     * <p>The refresh lock is acquired before invoking the internal merge. The caller
     * (engine) is expected to release the lock after applying catalog changes. On merge
     * failure the lock is released before propagating the exception.
     *
     * @param oneMerge       the merge to execute
     * @param mergeGeneration the writer generation for the merged output segment
     * @throws IOException if the merge fails
     */
    public void executeMerge(MergePolicy.OneMerge oneMerge, long mergeGeneration) throws IOException {
        // Wrap first so we can override onMergeComplete to coordinate with the engine's
        // refresh lock. Register the wrapper (not the original) — IndexWriter's _mergeInit
        // asserts registerDone on the OneMerge instance it receives.
        //
        // NOTE on locking order:
        //   refresh() path:  refreshLock → IndexWriter monitor (via addIndexes etc.)
        //   merge() path:    IndexWriter monitor (inside commitMerge) → refreshLock
        // Acquiring refreshLock only inside onMergeComplete inverts the order and deadlocks
        // against a concurrent refresh. Instead we acquire refreshLock BEFORE entering
        // merge() so this thread's order matches refresh()'s: refreshLock → IW monitor.
        // The lock is held for the duration of the merge (wider than strictly necessary)
        // and ownership transfers to the engine, which releases it after applyMergeChanges.
        MergePolicy.OneMerge wrapped = new MergePolicy.OneMerge(oneMerge) {
                @Override
                void onMergeComplete() throws IOException {
                    refreshLock.lock();
                    super.onMergeComplete();
            }
        };
        synchronized (this) {
            wrapped.mergeGen = mergeGeneration;
            wrapped.isExternal = false;
            wrapped.maxNumSegments = -1;
            wrapped.registerDone = true;
        }
        boolean handOff = false;
        try {
            // merge() must be called without holding the IW monitor — mergeInit asserts
            // !Thread.holdsLock(this).
            merge(wrapped);
            // Copy merge outputs from the wrapper back to the original OneMerge so callers
            // that still hold a reference to it (e.g. LuceneMerger reading getMergeInfo())
            // see the result.
            oneMerge.info = wrapped.info;
            handOff = true;
        } finally {
            // On failure, release so the engine's applyMergeChanges (which won't be called
            // for a failed merge) doesn't receive a lock to release. On success, the engine
            // owns the release.
            if (handOff == false && refreshLock.isHeldByCurrentThread()) {
                refreshLock.unlock();
            }
        }
    }

    @Override
    protected void mergeSuccess(MergePolicy.OneMerge merge) {
        // TODO update this for lucene as a primary engine
        // https://github.com/opensearch-project/OpenSearch/issues/21505
        super.mergeSuccess(merge);
    }
}
