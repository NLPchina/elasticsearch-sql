/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ObjectParser.ValueType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class ParsedScriptedMetric extends ParsedAggregation implements ScriptedMetric {
    private List<Object> aggregation;

    @Override
    public String getType() {
        return ScriptedMetricAggregationBuilder.NAME;
    }

    @Override
    public Object aggregation() {
        assert aggregation.size() == 1; // see InternalScriptedMetric#aggregations() for why we can assume this
        return aggregation.get(0);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return builder.field(CommonFields.VALUE.getPreferredName(), aggregation());
    }

    private static final ObjectParser<ParsedScriptedMetric, Void> PARSER = new ObjectParser<>(
            ParsedScriptedMetric.class.getSimpleName(),
            true,
            ParsedScriptedMetric::new
    );

    static {
        declareAggregationFields(PARSER);
        PARSER.declareField(
                (agg, value) -> agg.aggregation = Collections.singletonList(value),
                ParsedScriptedMetric::parseValue,
                CommonFields.VALUE,
                ValueType.VALUE_OBJECT_ARRAY
        );
    }

    private static Object parseValue(XContentParser parser) throws IOException {
        Token token = parser.currentToken();
        Object value = null;
        if (token == XContentParser.Token.VALUE_NULL) {
            value = null;
        } else if (token.isValue()) {
            if (token == XContentParser.Token.VALUE_STRING) {
                // binary values will be parsed back and returned as base64 strings when reading from json and yaml
                value = parser.text();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                value = parser.numberValue();
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                value = parser.booleanValue();
            } else if (token == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
                // binary values will be parsed back and returned as BytesArray when reading from cbor and smile
                value = new BytesArray(parser.binaryValue());
            }
        } else if (token == XContentParser.Token.START_OBJECT) {
            value = parser.map();
        } else if (token == XContentParser.Token.START_ARRAY) {
            value = parser.list();
        }
        return value;
    }

    public static ParsedScriptedMetric fromXContent(XContentParser parser, final String name) {
        ParsedScriptedMetric aggregation = PARSER.apply(parser, null);
        aggregation.setName(name);
        return aggregation;
    }
}
