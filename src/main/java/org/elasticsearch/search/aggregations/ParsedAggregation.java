/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations;

import com.google.common.collect.Maps;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.AbstractObjectParser;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An implementation of {@link Aggregation} that is parsed from a REST response.
 * Serves as a base class for all aggregation implementations that are parsed from REST.
 */
public abstract class ParsedAggregation extends InternalAggregation implements ToXContentFragment {

    private static final Field NAME_FIELD;

    static {
        try {
            NAME_FIELD = InternalAggregation.class.getDeclaredField("name");
            if (!NAME_FIELD.isAccessible()) {
                NAME_FIELD.setAccessible(true);
            }
        } catch (NoSuchFieldException e) {
            throw new IllegalStateException(e);
        }
    }

    protected static void declareAggregationFields(AbstractObjectParser<? extends ParsedAggregation, ?> objectParser) {
        objectParser.declareObject(
                ParsedAggregation::setMetadata,
                (parser, context) -> parser.map(),
                InternalAggregation.CommonFields.META
        );
    }

    protected ParsedAggregation() {
        super(null, Maps.newHashMap());
    }

    @Override
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
        return null;
    }

    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return false;
    }

    @Override
    public Object getProperty(List<String> path) {
        return null;
    }

    @Override
    public String getWriteableName() {
        return null;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
    }

    protected void setName(String name) {
        try {
            NAME_FIELD.set(this, name);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }

    protected void setMetadata(Map<String, Object> metadata) {
        getMetadata().clear();
        if (Objects.nonNull(metadata)) {
            getMetadata().putAll(metadata);
        }
    }

    /**
     * Parse a token of type XContentParser.Token.VALUE_NUMBER or XContentParser.Token.STRING to a double.
     * In other cases the default value is returned instead.
     */
    protected static double parseDouble(XContentParser parser, double defaultNullValue) throws IOException {
        Token currentToken = parser.currentToken();
        if (currentToken == XContentParser.Token.VALUE_NUMBER || currentToken == XContentParser.Token.VALUE_STRING) {
            return parser.doubleValue();
        } else {
            return defaultNullValue;
        }
    }
}
