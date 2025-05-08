/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.profile;

import org.elasticsearch.xcontent.InstantiatingObjectParser;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * The result of a profiled *thing*, like a query or an aggregation. See
 * {@link AbstractProfiler} for the statistic collection framework.
 */
public final class ParsedProfileResult {

    private static final InstantiatingObjectParser<ProfileResult, Void> PARSER;
    static {
        InstantiatingObjectParser.Builder<ProfileResult, Void> parser = InstantiatingObjectParser.builder(
                "profile_result",
                true,
                ProfileResult.class
        );
        parser.declareString(constructorArg(), ProfileResult.TYPE);
        parser.declareString(constructorArg(), ProfileResult.DESCRIPTION);
        parser.declareObject(
                constructorArg(),
                (p, c) -> p.map().entrySet().stream().collect(toMap(Map.Entry::getKey, e -> ((Number) e.getValue()).longValue())),
                ProfileResult.BREAKDOWN
        );
        parser.declareObject(optionalConstructorArg(), (p, c) -> p.map(), ProfileResult.DEBUG);
        parser.declareLong(constructorArg(), ProfileResult.NODE_TIME_RAW);
        parser.declareObjectArray(optionalConstructorArg(), (p, c) -> fromXContent(p), ProfileResult.CHILDREN);
        PARSER = parser.build();
    }

    public static ProfileResult fromXContent(XContentParser p) throws IOException {
        return PARSER.parse(p, null);
    }
}
