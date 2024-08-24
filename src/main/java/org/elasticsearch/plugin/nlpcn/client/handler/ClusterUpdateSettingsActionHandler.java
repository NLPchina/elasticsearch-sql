package org.elasticsearch.plugin.nlpcn.client.handler;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch.cluster.PutClusterSettingsRequest;
import co.elastic.clients.elasticsearch.cluster.PutClusterSettingsResponse;
import jakarta.json.stream.JsonParser;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.settings.ParsedClusterUpdateSettingsResponse;
import org.elasticsearch.common.Strings;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Optional;

/**
 * ClusterUpdateSettingsActionHandler
 *
 * @author shiyuan
 * @version V1.0
 * @since 2024-08-04 17:14
 */
public class ClusterUpdateSettingsActionHandler extends ActionHandler<ClusterUpdateSettingsRequest, PutClusterSettingsRequest, PutClusterSettingsResponse, ClusterUpdateSettingsResponse> {

    public ClusterUpdateSettingsActionHandler(ElasticsearchClient client) {
        super(client);
    }

    @Override
    public String getName() {
        return ClusterUpdateSettingsAction.NAME;
    }

    @Override
    protected PutClusterSettingsResponse doHandle(PutClusterSettingsRequest putClusterSettingsRequest) throws IOException {
        return client.cluster().putSettings(putClusterSettingsRequest);
    }

    @Override
    protected PutClusterSettingsRequest convertRequest(ClusterUpdateSettingsRequest clusterUpdateSettingsRequest) throws IOException {
        PutClusterSettingsRequest.Builder builder = new PutClusterSettingsRequest.Builder();
        try (Reader reader = new StringReader(Strings.toString(clusterUpdateSettingsRequest));
             JsonParser parser = jsonpMapper.jsonProvider().createParser(reader)) {
            builder.withJson(parser, jsonpMapper);
        }
        Optional.ofNullable(clusterUpdateSettingsRequest.masterNodeTimeout()).ifPresent(e -> builder.masterTimeout(Time.of(t -> t.time(e.toString()))));
        Optional.ofNullable(clusterUpdateSettingsRequest.ackTimeout()).ifPresent(e -> builder.timeout(Time.of(t -> t.time(e.toString()))));
        return builder.build();
    }

    @Override
    protected ClusterUpdateSettingsResponse convertResponse(PutClusterSettingsResponse putClusterSettingsResponse) throws IOException {
        return parseJson(putClusterSettingsResponse, ParsedClusterUpdateSettingsResponse::fromXContent);
    }
}
