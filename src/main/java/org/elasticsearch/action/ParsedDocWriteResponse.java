/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * A base class for the response of a write operation that involves a single doc
 */
public abstract class ParsedDocWriteResponse {

    /**
     * Parse the output of the {@link #innerToXContent(XContentBuilder, Params)} method.
     *
     * This method is intended to be called by subclasses and must be called multiple times to parse all the information concerning
     * {@link DocWriteResponse} objects. It always parses the current token, updates the given parsing context accordingly
     * if needed and then immediately returns.
     */
    public static void parseInnerToXContent(XContentParser parser, DocWriteResponse.Builder context) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);

        String currentFieldName = parser.currentName();
        token = parser.nextToken();

        if (token.isValue()) {
            if (DocWriteResponse._INDEX.equals(currentFieldName)) {
                // index uuid and shard id are unknown and can't be parsed back for now.
                context.setShardId(new ShardId(new Index(parser.text(), IndexMetadata.INDEX_UUID_NA_VALUE), -1));
            } else if (DocWriteResponse._ID.equals(currentFieldName)) {
                context.setId(parser.text());
            } else if (DocWriteResponse._VERSION.equals(currentFieldName)) {
                context.setVersion(parser.longValue());
            } else if (DocWriteResponse.RESULT.equals(currentFieldName)) {
                String result = parser.text();
                for (DocWriteResponse.Result r : DocWriteResponse.Result.values()) {
                    if (r.getLowercase().equals(result)) {
                        context.setResult(r);
                        break;
                    }
                }
            } else if (DocWriteResponse.FORCED_REFRESH.equals(currentFieldName)) {
                context.setForcedRefresh(parser.booleanValue());
            } else if (DocWriteResponse._SEQ_NO.equals(currentFieldName)) {
                context.setSeqNo(parser.longValue());
            } else if (DocWriteResponse._PRIMARY_TERM.equals(currentFieldName)) {
                context.setPrimaryTerm(parser.longValue());
            }
        } else if (token == XContentParser.Token.START_OBJECT) {
            if (DocWriteResponse._SHARDS.equals(currentFieldName)) {
                context.setShardInfo(DocWriteResponse.ShardInfo.fromXContent(parser));
            } else {
                parser.skipChildren(); // skip potential inner objects for forward compatibility
            }
        } else if (token == XContentParser.Token.START_ARRAY) {
            parser.skipChildren(); // skip potential inner arrays for forward compatibility
        }
    }
}
