/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * A base class for all the single bucket aggregations.
 */
public abstract class ParsedSingleBucketAggregation extends ParsedAggregation implements SingleBucketAggregation {

    private long docCount;
    protected InternalAggregations aggregations = InternalAggregations.EMPTY;

    @Override
    public long getDocCount() {
        return docCount;
    }

    protected void setDocCount(long docCount) {
        this.docCount = docCount;
    }

    @Override
    public InternalAggregations getAggregations() {
        return aggregations;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
        aggregations.toXContentInternal(builder, params);
        return builder;
    }

    protected static <T extends ParsedSingleBucketAggregation> T parseXContent(final XContentParser parser, T aggregation, String name)
            throws IOException {
        aggregation.setName(name);
        XContentParser.Token token = parser.currentToken();
        String currentFieldName = parser.currentName();
        if (token == XContentParser.Token.FIELD_NAME) {
            token = parser.nextToken();
        }
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);

        List<InternalAggregation> aggregations = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (CommonFields.DOC_COUNT.getPreferredName().equals(currentFieldName)) {
                    aggregation.setDocCount(parser.longValue());
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (CommonFields.META.getPreferredName().equals(currentFieldName)) {
                    aggregation.setMetadata(parser.map());
                } else {
                    XContentParserUtils.parseTypedKeysObject(
                            parser,
                            Aggregation.TYPED_KEYS_DELIMITER,
                            InternalAggregation.class,
                            aggregations::add
                    );
                }
            }
        }
        aggregation.aggregations = InternalAggregations.from(aggregations);
        return aggregation;
    }
}
