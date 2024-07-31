package org.elasticsearch.plugin.nlpcn.client.handler;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import jakarta.json.stream.JsonParser;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.common.Strings;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Optional;

/**
 * SearchActionHandler
 *
 * @author shiyuan
 * @version V1.0
 * @since 2024-08-04 22:12
 */
public class SearchActionHandler extends ActionHandler<org.elasticsearch.action.search.SearchRequest, SearchRequest, co.elastic.clients.elasticsearch.core.SearchResponse<Object>, SearchResponse> {

    public SearchActionHandler(ElasticsearchClient client) {
        super(client);
    }

    @Override
    public String getName() {
        return TransportSearchAction.NAME;
    }

    @Override
    protected co.elastic.clients.elasticsearch.core.SearchResponse<Object> doHandle(SearchRequest searchRequest) throws IOException {
        return client.search(searchRequest, Object.class);
    }

    @Override
    protected SearchRequest convertRequest(org.elasticsearch.action.search.SearchRequest searchRequest) throws IOException {
        SearchRequest.Builder builder = new SearchRequest.Builder();
        try (Reader reader = new StringReader(Strings.toString(searchRequest.source()));
             JsonParser parser = this.jsonpMapper.jsonProvider().createParser(reader)) {
            builder.withJson(parser, this.jsonpMapper);
        }
        builder.index(Arrays.asList(searchRequest.indices()));
        builder.requestCache(searchRequest.requestCache());
        builder.allowPartialSearchResults(searchRequest.allowPartialSearchResults());
        builder.ccsMinimizeRoundtrips(searchRequest.isCcsMinimizeRoundtrips());
        builder.preference(searchRequest.preference());
        builder.routing(searchRequest.routing());
        builder.searchType(getSearchType(searchRequest.searchType()));
        builder.batchedReduceSize((long) searchRequest.getBatchedReduceSize());
        builder.maxConcurrentShardRequests((long) searchRequest.getMaxConcurrentShardRequests());
        Optional.ofNullable(searchRequest.minCompatibleShardNode()).ifPresent(e -> builder.minCompatibleShardNode(e.toString()));
        Optional.ofNullable(searchRequest.scroll()).ifPresent(e -> builder.scroll(Time.of(t -> t.time(e.keepAlive().toString()))));
        Optional.ofNullable(searchRequest.indicesOptions()).ifPresent(options -> {
            builder.allowNoIndices(options.allowNoIndices());
            builder.ignoreUnavailable(options.ignoreUnavailable());
            builder.expandWildcards(getExpandWildcard(options.expandWildcards()));
        });
        return builder.build();
    }

    @Override
    protected SearchResponse convertResponse(co.elastic.clients.elasticsearch.core.SearchResponse<Object> searchResponse) throws IOException {
        return parseJson(searchResponse, SearchResponse::fromXContent);
    }
}
