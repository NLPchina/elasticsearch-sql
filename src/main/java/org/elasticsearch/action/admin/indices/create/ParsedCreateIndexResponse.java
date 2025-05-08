/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.action.support.master.ParsedShardsAcknowledgedResponse;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentParser;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A response for a create index action.
 */
public class ParsedCreateIndexResponse {

    private static final ConstructingObjectParser<CreateIndexResponse, Void> PARSER = new ConstructingObjectParser<>(
            "create_index",
            true,
            args -> new CreateIndexResponse((boolean) args[0], (boolean) args[1], (String) args[2])
    );

    static {
        declareFields(PARSER);
    }

    protected static <T extends CreateIndexResponse> void declareFields(ConstructingObjectParser<T, Void> objectParser) {
        ParsedShardsAcknowledgedResponse.declareAcknowledgedAndShardsAcknowledgedFields(objectParser);
        objectParser.declareField(constructorArg(), (parser, context) -> parser.textOrNull(), CreateIndexResponse.INDEX, ObjectParser.ValueType.STRING);
    }

    public static CreateIndexResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
