/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.nested;

import org.elasticsearch.search.aggregations.bucket.ParsedSingleBucketAggregation;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class ParsedReverseNested extends ParsedSingleBucketAggregation implements ReverseNested {

    @Override
    public String getType() {
        return ReverseNestedAggregationBuilder.NAME;
    }

    public static ParsedReverseNested fromXContent(XContentParser parser, final String name) throws IOException {
        return parseXContent(parser, new ParsedReverseNested(), name);
    }
}
