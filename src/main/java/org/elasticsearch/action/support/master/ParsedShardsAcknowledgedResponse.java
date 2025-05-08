/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.master;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class ParsedShardsAcknowledgedResponse {

    public static <T extends ShardsAcknowledgedResponse> void declareAcknowledgedAndShardsAcknowledgedFields(
            ConstructingObjectParser<T, Void> objectParser
    ) {
        ParsedAcknowledgedResponse.declareAcknowledgedField(objectParser);
        objectParser.declareField(
                constructorArg(),
                (parser, context) -> parser.booleanValue(),
                ShardsAcknowledgedResponse.SHARDS_ACKNOWLEDGED,
                ObjectParser.ValueType.BOOLEAN
        );
    }
}
