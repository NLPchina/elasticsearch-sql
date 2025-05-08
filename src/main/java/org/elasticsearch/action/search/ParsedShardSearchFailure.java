/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Represents a failure to search on a specific shard.
 */
public class ParsedShardSearchFailure {

    public static ShardSearchFailure fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token;
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        String currentFieldName = null;
        int shardId = -1;
        String indexName = null;
        String clusterAlias = null;
        String nodeId = null;
        ElasticsearchException exception = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (ShardSearchFailure.SHARD_FIELD.equals(currentFieldName)) {
                    shardId = parser.intValue();
                } else if (ShardSearchFailure.INDEX_FIELD.equals(currentFieldName)) {
                    String[] split = RemoteClusterAware.splitIndexName(parser.text());
                    clusterAlias = split[0];
                    indexName = split[1];
                } else if (ShardSearchFailure.NODE_FIELD.equals(currentFieldName)) {
                    nodeId = parser.text();
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (ShardSearchFailure.REASON_FIELD.equals(currentFieldName)) {
                    exception = ElasticsearchException.fromXContent(parser);
                } else {
                    parser.skipChildren();
                }
            } else {
                parser.skipChildren();
            }
        }
        SearchShardTarget searchShardTarget = null;
        if (nodeId != null) {
            searchShardTarget = new SearchShardTarget(
                    nodeId,
                    new ShardId(new Index(indexName, IndexMetadata.INDEX_UUID_NA_VALUE), shardId),
                    clusterAlias
            );
        }
        return new ShardSearchFailure(exception, searchShardTarget);
    }
}
