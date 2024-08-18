package org.elasticsearch.plugin.nlpcn.client.handler;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Conflicts;
import co.elastic.clients.elasticsearch._types.Slices;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch._types.WaitForActiveShards;
import co.elastic.clients.elasticsearch.core.DeleteByQueryRequest;
import co.elastic.clients.elasticsearch.core.DeleteByQueryResponse;
import jakarta.json.stream.JsonParser;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.ParsedBulkByScrollResponse;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * DeleteByQueryActionHandler
 *
 * @author shiyuan
 * @version V1.0
 * @since 2024-08-04 22:29
 */
public class DeleteByQueryActionHandler extends ActionHandler<org.elasticsearch.index.reindex.DeleteByQueryRequest, DeleteByQueryRequest, DeleteByQueryResponse, BulkByScrollResponse> {

    public DeleteByQueryActionHandler(ElasticsearchClient client) {
        super(client);
    }

    @Override
    public String getName() {
        return DeleteByQueryAction.NAME;
    }

    @Override
    protected DeleteByQueryResponse doHandle(DeleteByQueryRequest deleteByQueryRequest) throws IOException {
        return client.deleteByQuery(deleteByQueryRequest);
    }

    @Override
    protected DeleteByQueryRequest convertRequest(org.elasticsearch.index.reindex.DeleteByQueryRequest deleteByQueryRequest) throws IOException {
        DeleteByQueryRequest.Builder builder = new DeleteByQueryRequest.Builder();
        SearchRequest searchRequest = deleteByQueryRequest.getSearchRequest();
        if (Objects.nonNull(searchRequest)) {
            try (Reader reader = new StringReader(Strings.toString(deleteByQueryRequest));
                 JsonParser parser = this.jsonpMapper.jsonProvider().createParser(reader)) {
                builder.withJson(parser, this.jsonpMapper);
            }
            builder.requestCache(searchRequest.requestCache());
            builder.preference(searchRequest.preference());
            builder.searchType(getSearchType(searchRequest.searchType()));
        }
        builder.conflicts(deleteByQueryRequest.isAbortOnVersionConflict() ? Conflicts.Abort : Conflicts.Proceed);
        builder.index(Arrays.asList(deleteByQueryRequest.indices()));
        builder.routing(deleteByQueryRequest.getRouting());
        if (deleteByQueryRequest.getMaxDocs() > -1) {
            builder.maxDocs((long) deleteByQueryRequest.getMaxDocs());
        }
        builder.requestsPerSecond(deleteByQueryRequest.getRequestsPerSecond());
        builder.refresh(deleteByQueryRequest.isRefresh());
        ActiveShardCount waitForActiveShards = deleteByQueryRequest.getWaitForActiveShards();
        if (Objects.nonNull(waitForActiveShards) && waitForActiveShards.value() > -1) {
            builder.waitForActiveShards(WaitForActiveShards.of(w -> w.count(waitForActiveShards.value())));
        }
        builder.slices(Slices.of(s -> s.value(deleteByQueryRequest.getSlices())));
        Optional.ofNullable(deleteByQueryRequest.getScrollTime()).ifPresent(e -> builder.scroll(Time.of(t -> t.time(e.toString()))));
        Optional.ofNullable(deleteByQueryRequest.getTimeout()).ifPresent(e -> builder.timeout(Time.of(t -> t.time(e.toString()))));
        Optional.ofNullable(deleteByQueryRequest.indicesOptions()).ifPresent(options -> {
            builder.allowNoIndices(options.allowNoIndices());
            builder.ignoreUnavailable(options.ignoreUnavailable());
            builder.expandWildcards(getExpandWildcard(options.wildcardOptions()));
        });
        return builder.build();
    }

    @Override
    protected BulkByScrollResponse convertResponse(DeleteByQueryResponse deleteByQueryResponse) throws IOException {
        return parseJson(deleteByQueryResponse, ParsedBulkByScrollResponse::fromXContent);
    }
}
