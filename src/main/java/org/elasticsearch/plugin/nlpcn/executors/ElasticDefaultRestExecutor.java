package org.elasticsearch.plugin.nlpcn.executors;

import com.google.common.collect.Maps;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.reindex.BulkIndexByScrollResponseContentListener;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.plugin.nlpcn.*;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.search.SearchHits;
import org.nlpcn.es4sql.query.QueryAction;
import org.nlpcn.es4sql.query.SqlElasticRequestBuilder;
import org.nlpcn.es4sql.query.join.JoinRequestBuilder;
import org.nlpcn.es4sql.query.multi.MultiQueryRequestBuilder;

import java.io.IOException;
import java.util.Map;


public class ElasticDefaultRestExecutor implements RestExecutor {


    public ElasticDefaultRestExecutor() {
    }

    /**
     * Execute the ActionRequest and returns the REST response using the channel.
     */
    @Override
    public void execute(Client client, Map<String, String> params, QueryAction queryAction, RestChannel channel) throws Exception {
        SqlElasticRequestBuilder requestBuilder = queryAction.explain();
        ActionRequest request = requestBuilder.request();

        if (requestBuilder instanceof JoinRequestBuilder) {
            ElasticJoinExecutor executor = ElasticJoinExecutor.createJoinExecutor(client, requestBuilder);
            executor.run();
            executor.sendResponse(channel);
        } else if (requestBuilder instanceof MultiQueryRequestBuilder) {
            ElasticHitsExecutor executor = MultiRequestExecutorFactory.createExecutor(client, (MultiQueryRequestBuilder) requestBuilder);
            executor.run();
            sendDefaultResponse(executor.getHits(), channel);
        } else if (request instanceof SearchRequest) {
            client.search((SearchRequest) request, new RestStatusToXContentListener<SearchResponse>(channel));
        } else if (request instanceof DeleteByQueryRequest) {
            requestBuilder.getBuilder().execute(new BulkIndexByScrollResponseContentListener(channel, Maps.newHashMap()));
        } else if (request instanceof GetIndexRequest) {
            requestBuilder.getBuilder().execute(new GetIndexRequestRestListener(channel, (GetIndexRequest) request));
        } else {
            throw new Exception(String.format("Unsupported ActionRequest provided: %s", request.getClass().getName()));
        }
    }

    @Override
    public String execute(Client client, Map<String, String> params, QueryAction queryAction) throws Exception {

        SqlElasticRequestBuilder requestBuilder = queryAction.explain();
        ActionRequest request = requestBuilder.request();

        if (requestBuilder instanceof JoinRequestBuilder) {
            ElasticJoinExecutor executor = ElasticJoinExecutor.createJoinExecutor(client, requestBuilder);
            executor.run();
            return ElasticUtils.hitsAsStringResult(executor.getHits(), new MetaSearchResult());
        } else if (requestBuilder instanceof MultiQueryRequestBuilder) {
            ElasticHitsExecutor executor = MultiRequestExecutorFactory.createExecutor(client, (MultiQueryRequestBuilder) requestBuilder);
            executor.run();
            return ElasticUtils.hitsAsStringResult(executor.getHits(), new MetaSearchResult());
        } else if (request instanceof SearchRequest) {
            ActionFuture<SearchResponse> future = client.search((SearchRequest) request);
            SearchResponse response = future.actionGet();
            return response.toString();
        } else if (request instanceof DeleteByQueryRequest) {
            return requestBuilder.get().toString();
        } else if (request instanceof GetIndexRequest) {
            return requestBuilder.getBuilder().execute().actionGet().toString();
        } else {
            throw new Exception(String.format("Unsupported ActionRequest provided: %s", request.getClass().getName()));
        }

    }

    private void sendDefaultResponse(SearchHits hits, RestChannel channel) {
        try {
            String json = ElasticUtils.hitsAsStringResult(hits, new MetaSearchResult());
            BytesRestResponse bytesRestResponse = new BytesRestResponse(RestStatus.OK, json);
            channel.sendResponse(bytesRestResponse);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
