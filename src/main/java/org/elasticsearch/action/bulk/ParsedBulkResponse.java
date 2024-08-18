/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.throwUnknownField;
import static org.elasticsearch.common.xcontent.XContentParserUtils.throwUnknownToken;

/**
 * A response of a bulk execution. Holding a response for each item responding (in order) of the
 * bulk requests. Each item holds the index/type/id is operated on, and if it failed or not (with the
 * failure message).
 */
public class ParsedBulkResponse {

    public static BulkResponse fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);

        long took = -1L;
        long ingestTook = BulkResponse.NO_INGEST_TOOK;
        List<BulkItemResponse> items = new ArrayList<>();

        String currentFieldName = parser.currentName();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (BulkResponse.TOOK.equals(currentFieldName)) {
                    took = parser.longValue();
                } else if (BulkResponse.INGEST_TOOK.equals(currentFieldName)) {
                    ingestTook = parser.longValue();
                } else if (BulkResponse.ERRORS.equals(currentFieldName) == false) {
                    throwUnknownField(currentFieldName, parser);
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (BulkResponse.ITEMS.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        items.add(ParsedBulkItemResponse.fromXContent(parser, items.size()));
                    }
                } else {
                    throwUnknownField(currentFieldName, parser);
                }
            } else {
                throwUnknownToken(token, parser);
            }
        }
        return new BulkResponse(items.toArray(new BulkItemResponse[items.size()]), took, ingestTook);
    }
}
