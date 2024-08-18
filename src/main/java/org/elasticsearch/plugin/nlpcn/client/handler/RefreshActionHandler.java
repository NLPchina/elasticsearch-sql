package org.elasticsearch.plugin.nlpcn.client.handler;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.ParsedRefreshResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

/**
 * RefreshActionHandler
 *
 * @author shiyuan
 * @version V1.0
 * @since 2024-08-04 22:02
 */
public class RefreshActionHandler extends ActionHandler<org.elasticsearch.action.admin.indices.refresh.RefreshRequest, RefreshRequest, co.elastic.clients.elasticsearch.indices.RefreshResponse, BroadcastResponse> {

    public RefreshActionHandler(ElasticsearchClient client) {
        super(client);
    }

    @Override
    public String getName() {
        return RefreshAction.NAME;
    }

    @Override
    protected co.elastic.clients.elasticsearch.indices.RefreshResponse doHandle(RefreshRequest refreshRequest) throws IOException {
        return client.indices().refresh(refreshRequest);
    }

    @Override
    protected RefreshRequest convertRequest(org.elasticsearch.action.admin.indices.refresh.RefreshRequest refreshRequest) throws IOException {
        RefreshRequest.Builder builder = new RefreshRequest.Builder();
        builder.index(Arrays.asList(refreshRequest.indices()));
        Optional.ofNullable(refreshRequest.indicesOptions()).ifPresent(options -> {
            builder.allowNoIndices(options.allowNoIndices());
            builder.ignoreUnavailable(options.ignoreUnavailable());
            builder.expandWildcards(getExpandWildcard(options.wildcardOptions()));
        });
        return builder.build();
    }

    @Override
    protected BroadcastResponse convertResponse(co.elastic.clients.elasticsearch.indices.RefreshResponse refreshResponse) throws IOException {
        return parseJson(refreshResponse, ParsedRefreshResponse::fromXContent);
    }
}
