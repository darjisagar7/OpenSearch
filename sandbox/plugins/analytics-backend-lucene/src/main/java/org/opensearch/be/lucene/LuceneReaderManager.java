/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReader;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.opensearch.be.lucene.index.LuceneWriter.WRITER_GENERATION_ATTRIBUTE;

/**
 * Lucene implementation of {@link EngineReaderManager}.
 * <p>
 * Constructed with a {@link DataFormat} and an initial {@link DirectoryReader}
 * (typically opened from an IndexWriter). Maintains a map of {@link CatalogSnapshot}
 * to {@link DirectoryReader} so each snapshot gets the reader that was current
 * at the time of its refresh. On each {@link #afterRefresh}, the current reader is
 * refreshed via {@link DirectoryReader#openIfChanged}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneReaderManager implements EngineReaderManager<DirectoryReader> {

    private final DataFormat dataFormat;
    private final Map<CatalogSnapshot, DirectoryReader> readers = new HashMap<>();
    private volatile DirectoryReader currentReader;

    /**
     * Creates a new LuceneReaderManager.
     *
     * @param dataFormat the data format this reader manager serves
     * @param initialReader the initial DirectoryReader, must not be null
     * @throws NullPointerException if initialReader is null
     */
    public LuceneReaderManager(DataFormat dataFormat, DirectoryReader initialReader) {
        this.dataFormat = dataFormat;
        Objects.requireNonNull(initialReader, "initialReader must not be null");
        this.currentReader = initialReader;
    }

    @Override
    public DirectoryReader getReader(CatalogSnapshot catalogSnapshot) throws IOException {
        DirectoryReader reader = readers.get(catalogSnapshot);
        if (reader == null) {
            throw new IllegalStateException("No reader available for catalog snapshot [gen=" + catalogSnapshot.getGeneration() + "]");
        }
        return reader;
    }

    private Collection<Long> collectReferencedGenerations(Object reader) {
        DirectoryReader directoryReader = (DirectoryReader) reader;
        return directoryReader.leaves().stream().map(lrc -> {
            SegmentReader segmentReader = (SegmentReader) lrc.reader();
            SegmentCommitInfo sci = segmentReader.getSegmentInfo();
            return Long.parseLong(sci.info.getAttribute(WRITER_GENERATION_ATTRIBUTE));
        })
            .sorted()
            .toList();
    }

    @Override
    public void beforeRefresh() throws IOException {
        // no-op
    }

    @Override
    public void afterRefresh(boolean didRefresh, CatalogSnapshot catalogSnapshot) throws IOException {
        if (didRefresh == false || readers.containsKey(catalogSnapshot)) {
            return;
        }
        DirectoryReader refreshed = DirectoryReader.openIfChanged(currentReader);
        assert readersAreSame(catalogSnapshot, refreshed);
        if (refreshed != null) {
            currentReader = refreshed;
        }
        readers.put(catalogSnapshot, currentReader);
    }

    private boolean readersAreSame(CatalogSnapshot catalogSnapshot, DirectoryReader readers) {
        Collection<Long> generationsReferenced = catalogSnapshot.getSegments().stream().map(Segment::generation).sorted().toList();
        return generationsReferenced.equals(collectReferencedGenerations(readers));
    }

    @Override
    public void onDeleted(CatalogSnapshot catalogSnapshot) throws IOException {
        DirectoryReader reader = readers.remove(catalogSnapshot);
        if (reader != null) {
            reader.close();
        }
    }

    @Override
    public void onFilesDeleted(Collection<String> files) throws IOException {
        // no-op
    }

    @Override
    public void onFilesAdded(Collection<String> files) throws IOException {
        // no-op
    }

    @Override
    public void close() throws IOException {
        for (DirectoryReader reader : readers.values()) {
            reader.close();
        }
        readers.clear();
    }
}
