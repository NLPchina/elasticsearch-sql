/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile;

import org.elasticsearch.search.profile.aggregation.AggregationProfileShardResult;
import org.elasticsearch.search.profile.query.ParsedQueryProfileShardResult;
import org.elasticsearch.search.profile.query.QueryProfileShardResult;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Profile results for all shards.
 */
public final class ParsedSearchProfileResults {

    public static SearchProfileResults fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        Map<String, SearchProfileShardResult> profileResults = new HashMap<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.START_ARRAY) {
                if (SearchProfileResults.SHARDS_FIELD.equals(parser.currentName())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        parseProfileResultsEntry(parser, profileResults);
                    }
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                parser.skipChildren();
            }
        }
        return new SearchProfileResults(profileResults);
    }

    private static void parseProfileResultsEntry(XContentParser parser, Map<String, SearchProfileShardResult> searchProfileResults)
            throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        SearchProfileDfsPhaseResult searchProfileDfsPhaseResult = null;
        List<QueryProfileShardResult> queryProfileResults = new ArrayList<>();
        AggregationProfileShardResult aggProfileShardResult = null;
        ProfileResult fetchResult = null;
        String id = null;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (SearchProfileResults.ID_FIELD.equals(currentFieldName)) {
                    id = parser.text();
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("searches".equals(currentFieldName)) {
                    while ((parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        queryProfileResults.add(ParsedQueryProfileShardResult.fromXContent(parser));
                    }
                } else if (AggregationProfileShardResult.AGGREGATIONS.equals(currentFieldName)) {
                    aggProfileShardResult = AggregationProfileShardResult.fromXContent(parser);
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("dfs".equals(currentFieldName)) {
                    searchProfileDfsPhaseResult = ParsedSearchProfileDfsPhaseResult.fromXContent(parser);
                } else if ("fetch".equals(currentFieldName)) {
                    fetchResult = ProfileResult.fromXContent(parser);
                } else {
                    parser.skipChildren();
                }
            } else {
                parser.skipChildren();
            }
        }
        SearchProfileShardResult result = new SearchProfileShardResult(
                new SearchProfileQueryPhaseResult(queryProfileResults, aggProfileShardResult),
                fetchResult
        );
        result.getQueryPhase().setSearchProfileDfsPhaseResult(searchProfileDfsPhaseResult);
        searchProfileResults.put(id, result);
    }
}
