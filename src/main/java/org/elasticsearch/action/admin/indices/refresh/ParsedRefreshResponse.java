/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.refresh;

import org.elasticsearch.action.support.broadcast.BaseBroadcastResponse;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentParser;

import java.util.Arrays;

import static org.elasticsearch.action.support.broadcast.BaseBroadcastResponse.declareBroadcastFields;

/**
 * The response of a refresh action.
 */
public class ParsedRefreshResponse {

    private static final ConstructingObjectParser<BroadcastResponse, Void> PARSER = new ConstructingObjectParser<>("refresh", true, arg -> {
        BaseBroadcastResponse response = (BaseBroadcastResponse) arg[0];
        return new BroadcastResponse(
                response.getTotalShards(),
                response.getSuccessfulShards(),
                response.getFailedShards(),
                Arrays.asList(response.getShardFailures())
        );
    });

    static {
        declareBroadcastFields(PARSER);
    }

    public static BroadcastResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
