/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.refresh;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BaseBroadcastResponse;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * The response of a refresh action.
 */
public class ParsedRefreshResponse {

    private static final ParseField _SHARDS_FIELD = new ParseField("_shards");
    private static final ParseField TOTAL_FIELD = new ParseField("total");
    private static final ParseField SUCCESSFUL_FIELD = new ParseField("successful");
    private static final ParseField FAILED_FIELD = new ParseField("failed");
    private static final ParseField FAILURES_FIELD = new ParseField("failures");

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

    /**
     * {@link BaseBroadcastResponse#declareBroadcastFields(ConstructingObjectParser)}
     */
    @SuppressWarnings("unchecked")
    public static <T extends BaseBroadcastResponse> void declareBroadcastFields(ConstructingObjectParser<T, Void> PARSER) {
        ConstructingObjectParser<BaseBroadcastResponse, Void> shardsParser = new ConstructingObjectParser<>(
                "_shards",
                true,
                arg -> new BaseBroadcastResponse((int) arg[0], (int) arg[1], (int) arg[2], (List<DefaultShardOperationFailedException>) arg[3])
        );
        shardsParser.declareInt(constructorArg(), TOTAL_FIELD);
        shardsParser.declareInt(constructorArg(), SUCCESSFUL_FIELD);
        shardsParser.declareInt(constructorArg(), FAILED_FIELD);
        shardsParser.declareObjectArray(
                optionalConstructorArg(),
                (p, c) -> DefaultShardOperationFailedException.fromXContent(p),
                FAILURES_FIELD
        );
        PARSER.declareObject(constructorArg(), shardsParser, _SHARDS_FIELD);
    }
}
