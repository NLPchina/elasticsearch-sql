/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A multi search response.
 */
public class ParsedMultiSearchResponse {

    private static final ParseField RESPONSES = new ParseField(MultiSearchResponse.Fields.RESPONSES);
    private static final ParseField TOOK_IN_MILLIS = new ParseField("took");
    private static final Field REF_COUNTED_FIELD;

    static {
        try {
            REF_COUNTED_FIELD = MultiSearchResponse.class.getDeclaredField("refCounted");
            if (!REF_COUNTED_FIELD.isAccessible()) {
                REF_COUNTED_FIELD.setAccessible(true);
            }
        } catch (NoSuchFieldException e) {
            throw new IllegalStateException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<MultiSearchResponse, Void> PARSER = new ConstructingObjectParser<>(
            "multi_search",
            true,
            a -> new MultiSearchResponse(((List<MultiSearchResponse.Item>) a[0]).toArray(new MultiSearchResponse.Item[0]), (long) a[1])
    );
    static {
        PARSER.declareObjectArray(constructorArg(), (p, c) -> itemFromXContent(p), RESPONSES);
        PARSER.declareLong(constructorArg(), TOOK_IN_MILLIS);
    }

    public static MultiSearchResponse fromXContext(XContentParser parser) {
        return unpooled(PARSER.apply(parser, null));
    }

    private static MultiSearchResponse.Item itemFromXContent(XContentParser parser) throws IOException {
        // This parsing logic is a bit tricky here, because the multi search response itself is tricky:
        // 1) The json objects inside the responses array are either a search response or a serialized exception
        // 2) Each response json object gets a status field injected that ElasticsearchException.failureFromXContent(...) does not parse,
        // but SearchResponse.innerFromXContent(...) parses and then ignores. The status field is not needed to parse
        // the response item. However in both cases this method does need to parse the 'status' field otherwise the parsing of
        // the response item in the next json array element will fail due to parsing errors.

        MultiSearchResponse.Item item = null;
        String fieldName = null;

        Token token = parser.nextToken();
        assert token == Token.FIELD_NAME;
        outer: for (; token != Token.END_OBJECT; token = parser.nextToken()) {
            switch (token) {
                case FIELD_NAME:
                    fieldName = parser.currentName();
                    if ("error".equals(fieldName)) {
                        item = new MultiSearchResponse.Item(null, ElasticsearchException.failureFromXContent(parser));
                    } else if ("status".equals(fieldName) == false) {
                        item = new MultiSearchResponse.Item(ParsedSearchResponse.innerFromXContent(parser), null);
                        break outer;
                    }
                    break;
                case VALUE_NUMBER:
                    if ("status".equals(fieldName)) {
                        // Ignore the status value
                    }
                    break;
            }
        }
        assert parser.currentToken() == Token.END_OBJECT;
        return item;
    }

    private static MultiSearchResponse unpooled(MultiSearchResponse searchResponse) {
        MultiSearchResponse.Item[] items = searchResponse.getResponses();
        MultiSearchResponse.Item[] tempItems = Arrays.copyOf(items, items.length);
        searchResponse.decRef();
        System.arraycopy(tempItems, 0, items, 0, items.length);
        try {
            REF_COUNTED_FIELD.set(searchResponse, RefCounted.ALWAYS_REFERENCED);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
        return searchResponse;
    }
}
