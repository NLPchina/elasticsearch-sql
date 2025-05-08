package org.elasticsearch.plugin.nlpcn.client.handler;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.ParsedAcknowledgedResponse;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

/**
 * DeleteIndexActionHandler
 *
 * @author shiyuan
 * @version V1.0
 * @since 2024-08-04 21:30
 */
public class DeleteIndexActionHandler extends ActionHandler<org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest, DeleteIndexRequest, DeleteIndexResponse, AcknowledgedResponse> {

    public DeleteIndexActionHandler(ElasticsearchClient client) {
        super(client);
    }

    @Override
    public String getName() {
        return TransportDeleteIndexAction.TYPE.name();
    }

    @Override
    protected DeleteIndexResponse doHandle(DeleteIndexRequest deleteIndexRequest) throws IOException {
        return client.indices().delete(deleteIndexRequest);
    }

    @Override
    protected DeleteIndexRequest convertRequest(org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest deleteIndexRequest) throws IOException {
        DeleteIndexRequest.Builder builder = new DeleteIndexRequest.Builder();
        builder.index(Arrays.asList(deleteIndexRequest.indices()));
        Optional.ofNullable(deleteIndexRequest.masterNodeTimeout()).ifPresent(e -> builder.masterTimeout(Time.of(t -> t.time(e.toString()))));
        Optional.ofNullable(deleteIndexRequest.ackTimeout()).ifPresent(e -> builder.timeout(Time.of(t -> t.time(e.toString()))));
        Optional.ofNullable(deleteIndexRequest.indicesOptions()).ifPresent(options -> {
            builder.allowNoIndices(options.allowNoIndices());
            builder.ignoreUnavailable(options.ignoreUnavailable());
            builder.expandWildcards(getExpandWildcard(options.wildcardOptions()));
        });
        return builder.build();
    }

    @Override
    protected AcknowledgedResponse convertResponse(DeleteIndexResponse deleteIndexResponse) throws IOException {
        return parseJson(deleteIndexResponse, ParsedAcknowledgedResponse::fromXContent);
    }
}
