/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.action.support.master.ParsedAcknowledgedResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentParser;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A response for a cluster update settings action.
 */
public class ParsedClusterUpdateSettingsResponse {

    private static final ConstructingObjectParser<ClusterUpdateSettingsResponse, Void> PARSER = new ConstructingObjectParser<>(
            "cluster_update_settings_response",
            true,
            args -> {
                return new ClusterUpdateSettingsResponse((boolean) args[0], (Settings) args[1], (Settings) args[2]);
            }
    );

    static {
        ParsedAcknowledgedResponse.declareAcknowledgedField(PARSER);
        PARSER.declareObject(constructorArg(), (p, c) -> Settings.fromXContent(p), ClusterUpdateSettingsResponse.TRANSIENT);
        PARSER.declareObject(constructorArg(), (p, c) -> Settings.fromXContent(p), ClusterUpdateSettingsResponse.PERSISTENT);
    }

    public static ClusterUpdateSettingsResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
