package org.elasticsearch.plugin.nlpcn.client.handler;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.MsearchRequest;
import co.elastic.clients.elasticsearch.core.MsearchResponse;
import co.elastic.clients.elasticsearch.core.msearch.MultisearchBody;
import co.elastic.clients.elasticsearch.core.msearch.MultisearchHeader;
import co.elastic.clients.elasticsearch.core.msearch.RequestItem;
import jakarta.json.stream.JsonParser;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportMultiSearchAction;
import org.elasticsearch.common.Strings;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Optional;

/**
 * MultiSearchActionHandler
 *
 * @author shiyuan
 * @version V1.0
 * @since 2024-08-04 22:25
 */
public class MultiSearchActionHandler extends ActionHandler<MultiSearchRequest, MsearchRequest, MsearchResponse<Object>, MultiSearchResponse> {

    public MultiSearchActionHandler(ElasticsearchClient client) {
        super(client);
    }

    @Override
    public String getName() {
        return TransportMultiSearchAction.TYPE.name();
    }

    @Override
    protected MsearchResponse<Object> doHandle(MsearchRequest msearchRequest) throws IOException {
        return client.msearch(msearchRequest, Object.class);
    }

    @Override
    protected MsearchRequest convertRequest(MultiSearchRequest multiSearchRequest) throws IOException {
        MsearchRequest.Builder builder = new MsearchRequest.Builder();
        if (multiSearchRequest.maxConcurrentSearchRequests() > 0) {
            builder.maxConcurrentSearches((long) multiSearchRequest.maxConcurrentSearchRequests());
        }
        for (SearchRequest request : multiSearchRequest.requests()) {
            MultisearchHeader.Builder msearchHeaderBuilder = new MultisearchHeader.Builder();
            msearchHeaderBuilder.index(Arrays.asList(request.indices()));
            msearchHeaderBuilder.requestCache(request.requestCache());
            msearchHeaderBuilder.allowPartialSearchResults(request.allowPartialSearchResults());
            msearchHeaderBuilder.ccsMinimizeRoundtrips(request.isCcsMinimizeRoundtrips());
            msearchHeaderBuilder.preference(request.preference());
            msearchHeaderBuilder.routing(request.routing());
            msearchHeaderBuilder.searchType(getSearchType(request.searchType()));
            Optional.ofNullable(request.indicesOptions()).ifPresent(options -> {
                msearchHeaderBuilder.allowNoIndices(options.allowNoIndices());
                msearchHeaderBuilder.ignoreUnavailable(options.ignoreUnavailable());
                msearchHeaderBuilder.expandWildcards(getExpandWildcard(options.expandWildcards()));
                msearchHeaderBuilder.ignoreThrottled(options.ignoreThrottled());
            });

            MultisearchBody.Builder msearchBodyBuilder = new MultisearchBody.Builder();
            try (Reader reader = new StringReader(Strings.toString(request.source()));
                 JsonParser parser = this.jsonpMapper.jsonProvider().createParser(reader)) {
                msearchBodyBuilder.withJson(parser, this.jsonpMapper);
            }
            RequestItem.Builder requestItemBuilder = new RequestItem.Builder();
            requestItemBuilder.header(msearchHeaderBuilder.build());
            requestItemBuilder.body(msearchBodyBuilder.build());
            builder.searches(requestItemBuilder.build());
        }
        Optional.ofNullable(multiSearchRequest.indicesOptions()).ifPresent(options -> {
            builder.allowNoIndices(options.allowNoIndices());
            builder.ignoreUnavailable(options.ignoreUnavailable());
            builder.expandWildcards(getExpandWildcard(options.expandWildcards()));
        });
        return builder.build();
    }

    @Override
    protected MultiSearchResponse convertResponse(MsearchResponse<Object> msearchResponse) throws IOException {
        return parseJson(msearchResponse, MultiSearchResponse::fromXContext);
    }
}
