/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.common.xcontent.XContentParserUtils.parseTypedKeysObject;

/**
 * Represents a set of {@link InternalAggregation}s
 */
public final class ParsedInternalAggregations {

    public static InternalAggregations fromXContent(XContentParser parser) throws IOException {
        final List<InternalAggregation> aggregations = new ArrayList<>();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.START_OBJECT) {
                SetOnce<InternalAggregation> typedAgg = new SetOnce<>();
                String currentField = parser.currentName();
                parseTypedKeysObject(parser, Aggregation.TYPED_KEYS_DELIMITER, InternalAggregation.class, typedAgg::set);
                if (typedAgg.get() != null) {
                    aggregations.add(typedAgg.get());
                } else {
                    throw new ParsingException(
                            parser.getTokenLocation(),
                            String.format(Locale.ROOT, "Could not parse aggregation keyed as [%s]", currentField)
                    );
                }
            }
        }
        return InternalAggregations.from(aggregations);
    }
}
