/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.search.SearchHits.parseTotalHitsFragment;

public final class ParsedSearchHits {

    public static SearchHits fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            parser.nextToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        }
        XContentParser.Token token = parser.currentToken();
        String currentFieldName = null;
        List<SearchHit> hits = new ArrayList<>();
        TotalHits totalHits = null;
        float maxScore = 0f;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (SearchHits.Fields.TOTAL.equals(currentFieldName)) {
                    // For BWC with nodes pre 7.0
                    long value = parser.longValue();
                    totalHits = value == -1 ? null : new TotalHits(value, Relation.EQUAL_TO);
                } else if (SearchHits.Fields.MAX_SCORE.equals(currentFieldName)) {
                    maxScore = parser.floatValue();
                }
            } else if (token == XContentParser.Token.VALUE_NULL) {
                if (SearchHits.Fields.MAX_SCORE.equals(currentFieldName)) {
                    maxScore = Float.NaN; // NaN gets rendered as null-field
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (SearchHits.Fields.HITS.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        hits.add(ParsedSearchHit.fromXContent(parser));
                    }
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (SearchHits.Fields.TOTAL.equals(currentFieldName)) {
                    totalHits = parseTotalHitsFragment(parser);
                } else {
                    parser.skipChildren();
                }
            }
        }
        return SearchHits.unpooled(hits.toArray(SearchHits.EMPTY), totalHits, maxScore);
    }
}
