/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.index.reindex.ScrollableHitSource.SearchFailure;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Response used for actions that index many documents using a scroll request.
 */
public class ParsedBulkByScrollResponse {

    @SuppressWarnings("unchecked")
    private static final ObjectParser<BulkByScrollResponseBuilder, Void> PARSER = new ObjectParser<>(
            "bulk_by_scroll_response",
            true,
            BulkByScrollResponseBuilder::new
    );
    static {
        PARSER.declareLong(BulkByScrollResponseBuilder::setTook, new ParseField(BulkByScrollResponse.TOOK_FIELD));
        PARSER.declareBoolean(BulkByScrollResponseBuilder::setTimedOut, new ParseField(BulkByScrollResponse.TIMED_OUT_FIELD));
        PARSER.declareObjectArray(BulkByScrollResponseBuilder::setFailures, (p, c) -> parseFailure(p), new ParseField(BulkByScrollResponse.FAILURES_FIELD));
        // since the result of BulkByScrollResponse.Status are mixed we also parse that in this
        ParsedBulkByScrollTask.ParsedStatus.declareFields(PARSER);
    }

    public static BulkByScrollResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null).buildResponse();
    }

    private static Object parseFailure(XContentParser parser) throws IOException {
        ensureExpectedToken(Token.START_OBJECT, parser.currentToken(), parser);
        Token token;
        String index = null;
        String id = null;
        Integer status = null;
        Integer shardId = null;
        String nodeId = null;
        ElasticsearchException bulkExc = null;
        ElasticsearchException searchExc = null;
        while ((token = parser.nextToken()) != Token.END_OBJECT) {
            ensureExpectedToken(Token.FIELD_NAME, token, parser);
            String name = parser.currentName();
            token = parser.nextToken();
            if (token == Token.START_ARRAY) {
                parser.skipChildren();
            } else if (token == Token.START_OBJECT) {
                switch (name) {
                    case SearchFailure.REASON_FIELD -> searchExc = ElasticsearchException.fromXContent(parser);
                    case Failure.CAUSE_FIELD -> bulkExc = ElasticsearchException.fromXContent(parser);
                    default -> parser.skipChildren();
                }
            } else if (token == Token.VALUE_STRING) {
                switch (name) {
                    // This field is the same as SearchFailure.index
                    case Failure.INDEX_FIELD -> index = parser.text();
                    case Failure.ID_FIELD -> id = parser.text();
                    case SearchFailure.NODE_FIELD -> nodeId = parser.text();
                }
            } else if (token == Token.VALUE_NUMBER) {
                switch (name) {
                    case Failure.STATUS_FIELD -> status = parser.intValue();
                    case SearchFailure.SHARD_FIELD -> shardId = parser.intValue();
                }
            }
        }
        if (bulkExc != null) {
            return new Failure(index, id, bulkExc, RestStatus.fromCode(status));
        } else if (searchExc != null) {
            if (status == null) {
                return new SearchFailure(searchExc, index, shardId, nodeId);
            } else {
                return new SearchFailure(searchExc, index, shardId, nodeId, RestStatus.fromCode(status));
            }
        } else {
            throw new ElasticsearchParseException("failed to parse failures array. At least one of {reason,cause} must be present");
        }
    }
}
