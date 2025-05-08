/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.profile.query;

import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Public interface and serialization container for profiled timings of the
 * Collectors used in the search.  Children CollectorResult's may be
 * embedded inside of a parent CollectorResult
 */
public class ParsedCollectorResult {

    public static CollectorResult fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        String currentFieldName = null;
        String name = null, reason = null;
        long time = -1;
        List<CollectorResult> children = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (CollectorResult.NAME.match(currentFieldName, parser.getDeprecationHandler())) {
                    name = parser.text();
                } else if (CollectorResult.REASON.match(currentFieldName, parser.getDeprecationHandler())) {
                    reason = parser.text();
                } else if (CollectorResult.TIME.match(currentFieldName, parser.getDeprecationHandler())) {
                    // we need to consume this value, but we use the raw nanosecond value
                    parser.text();
                } else if (CollectorResult.TIME_NANOS.match(currentFieldName, parser.getDeprecationHandler())) {
                    time = parser.longValue();
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (CollectorResult.CHILDREN.match(currentFieldName, parser.getDeprecationHandler())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        children.add(ParsedCollectorResult.fromXContent(parser));
                    }
                } else {
                    parser.skipChildren();
                }
            } else {
                parser.skipChildren();
            }
        }
        return new CollectorResult(name, reason, time, children);
    }
}
