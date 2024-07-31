package org.elasticsearch.plugin.nlpcn.client.handler;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch.indices.PutMappingRequest;
import co.elastic.clients.elasticsearch.indices.PutMappingResponse;
import jakarta.json.stream.JsonParser;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Optional;

/**
 * PutMappingActionHandler
 *
 * @author shiyuan
 * @version V1.0
 * @since 2024-08-04 21:37
 */
public class PutMappingActionHandler extends ActionHandler<org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest, PutMappingRequest, PutMappingResponse, AcknowledgedResponse> {

    public PutMappingActionHandler(ElasticsearchClient client) {
        super(client);
    }

    @Override
    public String getName() {
        return PutMappingAction.NAME;
    }

    @Override
    protected PutMappingResponse doHandle(PutMappingRequest putMappingRequest) throws IOException {
        return client.indices().putMapping(putMappingRequest);
    }

    @Override
    protected PutMappingRequest convertRequest(org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest putMappingRequest) throws IOException {
        PutMappingRequest.Builder builder = new PutMappingRequest.Builder();
        if (Strings.hasLength(putMappingRequest.source())) {
            try (Reader reader = new StringReader(putMappingRequest.source());
                 JsonParser parser = this.jsonpMapper.jsonProvider().createParser(reader)) {
                builder.withJson(parser, this.jsonpMapper);
            }
        }
        builder.index(Arrays.asList(putMappingRequest.indices()));
        builder.writeIndexOnly(putMappingRequest.writeIndexOnly());
        Optional.ofNullable(putMappingRequest.indicesOptions()).ifPresent(options -> {
            builder.allowNoIndices(options.allowNoIndices());
            builder.ignoreUnavailable(options.ignoreUnavailable());
            builder.expandWildcards(getExpandWildcard(options.expandWildcards()));
        });
        Optional.ofNullable(putMappingRequest.timeout()).ifPresent(e -> builder.timeout(Time.of(t -> t.time(e.toString()))));
        Optional.ofNullable(putMappingRequest.masterNodeTimeout()).ifPresent(e -> builder.masterTimeout(Time.of(t -> t.time(e.toString()))));
        return builder.build();
    }

    @Override
    protected AcknowledgedResponse convertResponse(PutMappingResponse putMappingResponse) throws IOException {
        return parseJson(putMappingResponse, AcknowledgedResponse::fromXContent);
    }
}
