/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile.query;

import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * A container class to hold the profile results for a single shard in the request.
 * Contains a list of query profiles, a collector tree and a total rewrite tree.
 */
public final class ParsedQueryProfileShardResult {

    public static QueryProfileShardResult fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        String currentFieldName = null;
        List<ProfileResult> queryProfileResults = new ArrayList<>();
        long rewriteTime = 0;
        Long vectorOperationsCount = null;
        CollectorResult collector = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (QueryProfileShardResult.REWRITE_TIME.equals(currentFieldName)) {
                    rewriteTime = parser.longValue();
                } else if (QueryProfileShardResult.VECTOR_OPERATIONS_COUNT.equals(currentFieldName)) {
                    vectorOperationsCount = parser.longValue();
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (QueryProfileShardResult.QUERY_ARRAY.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        queryProfileResults.add(ProfileResult.fromXContent(parser));
                    }
                } else if (QueryProfileShardResult.COLLECTOR.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        collector = CollectorResult.fromXContent(parser);
                    }
                } else {
                    parser.skipChildren();
                }
            } else {
                parser.skipChildren();
            }
        }
        return new QueryProfileShardResult(queryProfileResults, rewriteTime, collector, vectorOperationsCount);
    }
}
