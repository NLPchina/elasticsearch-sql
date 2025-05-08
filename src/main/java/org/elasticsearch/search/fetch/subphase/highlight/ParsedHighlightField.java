/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.subphase.highlight;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * A field highlighted with its highlighted fragments.
 */
public class ParsedHighlightField {

    public static HighlightField fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
        String fieldName = parser.currentName();
        Text[] fragments;
        XContentParser.Token token = parser.nextToken();
        if (token == XContentParser.Token.START_ARRAY) {
            List<Text> values = new ArrayList<>();
            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                values.add(new Text(parser.text()));
            }
            fragments = values.toArray(Text.EMPTY_ARRAY);
        } else if (token == XContentParser.Token.VALUE_NULL) {
            fragments = null;
        } else {
            throw new ParsingException(parser.getTokenLocation(), "unexpected token type [" + token + "]");
        }
        return new HighlightField(fieldName, fragments);
    }

}
