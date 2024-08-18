package org.elasticsearch.plugin.nlpcn.client.handler;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch.core.ScrollRequest;
import co.elastic.clients.elasticsearch.core.ScrollResponse;
import org.elasticsearch.action.search.ParsedSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.TransportSearchScrollAction;

import java.io.IOException;
import java.util.Optional;

/**
 * SearchScrollActionHandler
 *
 * @author shiyuan
 * @version V1.0
 * @since 2024-08-04 22:21
 */
public class SearchScrollActionHandler extends ActionHandler<SearchScrollRequest, ScrollRequest, ScrollResponse<Object>, SearchResponse> {

    public SearchScrollActionHandler(ElasticsearchClient client) {
        super(client);
    }

    @Override
    public String getName() {
        return TransportSearchScrollAction.TYPE.name();
    }

    @Override
    protected ScrollResponse<Object> doHandle(ScrollRequest scrollRequest) throws IOException {
        return client.scroll(scrollRequest, Object.class);
    }

    @Override
    protected ScrollRequest convertRequest(SearchScrollRequest searchScrollRequest) throws IOException {
        ScrollRequest.Builder builder = new ScrollRequest.Builder();
        builder.scrollId(searchScrollRequest.scrollId());
        Optional.ofNullable(searchScrollRequest.scroll()).ifPresent(e -> builder.scroll(Time.of(t -> t.time(e.keepAlive().toString()))));
        return builder.build();
    }

    @Override
    protected SearchResponse convertResponse(ScrollResponse<Object> scrollResponse) throws IOException {
        return parseJson(scrollResponse, ParsedSearchResponse::fromXContent);
    }
}
