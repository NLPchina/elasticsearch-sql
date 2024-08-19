/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.delete;

import org.elasticsearch.action.ParsedDocWriteResponse;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * The response of the delete action.
 *
 * @see org.elasticsearch.action.delete.DeleteRequest
 * @see org.elasticsearch.client.internal.Client#delete(DeleteRequest)
 */
public class ParsedDeleteResponse {

    public static DeleteResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);

        DeleteResponse.Builder context = new DeleteResponse.Builder();
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            parseXContentFields(parser, context);
        }
        return context.build();
    }

    /**
     * Parse the current token and update the parsing context appropriately.
     */
    public static void parseXContentFields(XContentParser parser, DeleteResponse.Builder context) throws IOException {
        ParsedDocWriteResponse.parseInnerToXContent(parser, context);
    }
}
