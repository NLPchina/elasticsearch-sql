package org.elasticsearch.plugin.nlpcn.executors;

import com.google.common.collect.Maps;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.reindex.BulkIndexByScrollResponseContentListener;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.plugin.nlpcn.ElasticHitsExecutor;
import org.elasticsearch.plugin.nlpcn.ElasticJoinExecutor;
import org.elasticsearch.plugin.nlpcn.ElasticUtils;
import org.elasticsearch.plugin.nlpcn.GetIndexRequestRestListener;
import org.elasticsearch.plugin.nlpcn.MetaSearchResult;
import org.elasticsearch.plugin.nlpcn.MultiRequestExecutorFactory;
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
     * zhongshu-comment 第二个参数Map<String, String> params 并没有被使用
     */
    @Override
    public void execute(Client client, Map<String, String> params, QueryAction queryAction, RestChannel channel) throws Exception {
        //zhongshu-comment queryAction的使命结束了，交由SqlElasticRequestBuilder接力，SqlElasticRequestBuilder是es-sql自己定义的一个类，不是es原生api
        SqlElasticRequestBuilder requestBuilder = queryAction.explain();

        //zhongshu-comment 看这行，将QueryAction对象转为es查询对象，这个是重点了，到这步就已经成功将sql字符串转化为es查询请求了
        //zhongshu-comment ActionRequest是es的原生api
        ActionRequest request = requestBuilder.request();

        //zhongshu-comment 应该是分别对应6中QueryAction子类实现
        if (requestBuilder instanceof JoinRequestBuilder) { //zhongshu-comment 对应连接查询：ESJoinQueryAction
            ElasticJoinExecutor executor = ElasticJoinExecutor.createJoinExecutor(client, requestBuilder);
            executor.run();
            executor.sendResponse(channel);
        } else if (requestBuilder instanceof MultiQueryRequestBuilder) { //zhongshu-comment 对应union查询：MultiQueryAction
            ElasticHitsExecutor executor = MultiRequestExecutorFactory.createExecutor(client, (MultiQueryRequestBuilder) requestBuilder);
            executor.run();
            sendDefaultResponse(executor.getHits(), channel);
        } else if (request instanceof SearchRequest) {
            //zhongshu-comment 对应的QueryAction实现子类：DefaultQueryAction、AggregationQueryAction
            //zhongshu-comment 对应的SqlElasticRequestBuilder实现子类：SqlElasticSearchRequestBuilder
            client.search((SearchRequest) request, new RestStatusToXContentListener<>(channel));
        } else if (request instanceof DeleteByQueryRequest) {
            //zhongshu-comment 对应的QueryAction实现子类：DeleteQueryAction
            //zhongshu-comment 对应的SqlElasticRequestBuilder实现子类：SqlElasticDeleteByQueryRequestBuilder
            requestBuilder.getBuilder().execute(new BulkIndexByScrollResponseContentListener(channel, Maps.newHashMap()));
        } else if (request instanceof GetIndexRequest) {
            //zhongshu-comment 对应的QueryAction实现子类：ShowQueryAction
            //zhongshu-comment 对应的SqlElasticRequestBuilder实现子类：是一个匿名内部类，跳进去queryAction.explain()看
            requestBuilder.getBuilder().execute(new GetIndexRequestRestListener(channel, (GetIndexRequest) request));
        } else if (request instanceof SearchScrollRequest) {
            client.searchScroll((SearchScrollRequest) request, new RestStatusToXContentListener<>(channel));
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
            return BytesReference.bytes(ElasticUtils.hitsAsXContentBuilder(executor.getHits(), new MetaSearchResult())).utf8ToString();
        } else if (requestBuilder instanceof MultiQueryRequestBuilder) {
            ElasticHitsExecutor executor = MultiRequestExecutorFactory.createExecutor(client, (MultiQueryRequestBuilder) requestBuilder);
            executor.run();
            return BytesReference.bytes(ElasticUtils.hitsAsXContentBuilder(executor.getHits(), new MetaSearchResult())).utf8ToString();
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
            XContentBuilder builder = ElasticUtils.hitsAsXContentBuilder(hits, new MetaSearchResult());
            BytesRestResponse bytesRestResponse = new BytesRestResponse(RestStatus.OK, builder);
            channel.sendResponse(bytesRestResponse);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
