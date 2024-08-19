/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.update;

import org.elasticsearch.action.ParsedDocWriteResponse;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class ParsedUpdateResponse {

    public static UpdateResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);

        UpdateResponse.Builder context = new UpdateResponse.Builder();
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            parseXContentFields(parser, context);
        }
        return context.build();
    }

    /**
     * Parse the current token and update the parsing context appropriately.
     */
    public static void parseXContentFields(XContentParser parser, UpdateResponse.Builder context) throws IOException {
        XContentParser.Token token = parser.currentToken();
        String currentFieldName = parser.currentName();

        if (UpdateResponse.GET.equals(currentFieldName)) {
            if (token == XContentParser.Token.START_OBJECT) {
                context.setGetResult(GetResult.fromXContentEmbedded(parser));
            }
        } else {
            ParsedDocWriteResponse.parseInnerToXContent(parser, context);
        }
    }
}
