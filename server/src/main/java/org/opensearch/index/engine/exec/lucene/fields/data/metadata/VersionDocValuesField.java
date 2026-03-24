/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.fields.data.metadata;

import org.apache.lucene.document.NumericDocValuesField;
import org.opensearch.index.engine.exec.FieldCapability;
import org.opensearch.index.engine.exec.lucene.fields.LuceneField;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParseContext;

import java.util.EnumSet;
import java.util.Set;

/**
 * LuceneField for _version, _seq_no, and _primary_term metadata fields.
 * Uses NumericDocValuesField (not SortedNumericDocValuesField) to match
 * what VersionsAndSeqNoResolver expects via reader.getNumericDocValues().
 */
public class VersionDocValuesField extends LuceneField {

    @Override
    public void createField(MappedFieldType fieldType, ParseContext.Document document, Object parseValue) {
        final Number value = (Number) parseValue;
        if (fieldType.hasDocValues()) {
            document.add(new NumericDocValuesField(fieldType.name(), value.longValue()));
        }
    }

    @Override
    public Set<FieldCapability> getFieldCapabilities() {
        return EnumSet.of(FieldCapability.DOC_VALUES);
    }
}
