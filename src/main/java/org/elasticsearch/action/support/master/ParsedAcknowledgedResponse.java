/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.support.master;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A response to an action which updated the cluster state, but needs to report whether any relevant nodes failed to apply the update. For
 * instance, a {@link org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest} may update a mapping in the index metadata, but
 * one or more data nodes may fail to acknowledge the new mapping within the ack timeout. If this happens then clients must accept that
 * subsequent requests that rely on the mapping update may return errors from the lagging data nodes.
 * <p>
 * Actions which return a payload-free acknowledgement of success should generally prefer to use {@link ActionResponse.Empty} instead of
 * {@link AcknowledgedResponse}, and other listeners should generally prefer {@link Void}.
 */
public class ParsedAcknowledgedResponse {

    private static final ParseField ACKNOWLEDGED = new ParseField(AcknowledgedResponse.ACKNOWLEDGED_KEY);

    public static <T extends AcknowledgedResponse> void declareAcknowledgedField(ConstructingObjectParser<T, Void> objectParser) {
        objectParser.declareField(
                constructorArg(),
                (parser, context) -> parser.booleanValue(),
                ACKNOWLEDGED,
                ObjectParser.ValueType.BOOLEAN
        );
    }

    /**
     * A generic parser that simply parses the acknowledged flag
     */
    private static final ConstructingObjectParser<Boolean, Void> ACKNOWLEDGED_FLAG_PARSER = new ConstructingObjectParser<>(
            "acknowledged_flag",
            true,
            args -> (Boolean) args[0]
    );

    static {
        ACKNOWLEDGED_FLAG_PARSER.declareField(
                constructorArg(),
                (parser, context) -> parser.booleanValue(),
                ACKNOWLEDGED,
                ObjectParser.ValueType.BOOLEAN
        );
    }

    public static AcknowledgedResponse fromXContent(XContentParser parser) throws IOException {
        return AcknowledgedResponse.of(ACKNOWLEDGED_FLAG_PARSER.apply(parser, null));
    }
}
