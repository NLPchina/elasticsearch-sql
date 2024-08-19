/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile;

import org.elasticsearch.search.profile.query.ParsedQueryProfileShardResult;
import org.elasticsearch.xcontent.InstantiatingObjectParser;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class ParsedSearchProfileDfsPhaseResult {

    private static final InstantiatingObjectParser<SearchProfileDfsPhaseResult, Void> PARSER;

    static {
        InstantiatingObjectParser.Builder<SearchProfileDfsPhaseResult, Void> parser = InstantiatingObjectParser.builder(
                "search_profile_dfs_phase_result",
                true,
                SearchProfileDfsPhaseResult.class
        );
        parser.declareObject(optionalConstructorArg(), (p, c) -> ProfileResult.fromXContent(p), SearchProfileDfsPhaseResult.STATISTICS);
        parser.declareObjectArray(optionalConstructorArg(), (p, c) -> ParsedQueryProfileShardResult.fromXContent(p), SearchProfileDfsPhaseResult.KNN);
        PARSER = parser.build();
    }

    public static SearchProfileDfsPhaseResult fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
