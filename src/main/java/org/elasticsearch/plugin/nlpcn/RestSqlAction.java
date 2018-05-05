package org.elasticsearch.plugin.nlpcn;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugin.nlpcn.executors.ActionRequestRestExecuterFactory;
import org.elasticsearch.plugin.nlpcn.executors.RestExecutor;
import org.elasticsearch.rest.*;
import org.nlpcn.es4sql.SearchDao;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.QueryAction;

import java.io.IOException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.*;


public class RestSqlAction extends BaseRestHandler {

//    public static final RestSqlAction INSTANCE = new RestSqlAction();


    public RestSqlAction(Settings settings, RestController restController) {
        super(settings);
        restController.registerHandler(RestRequest.Method.POST, "/_sql/_explain", this);
        restController.registerHandler(RestRequest.Method.GET, "/_sql/_explain", this);
        restController.registerHandler(RestRequest.Method.POST, "/_sql", this);
        restController.registerHandler(RestRequest.Method.GET, "/_sql", this);
    }

    @Override
    public String getName() {
        return "sql_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String sql = request.param("sql");

        if (sql == null) {
            sql = request.content().utf8ToString();
        }
        try {
            SearchDao searchDao = new SearchDao(client);
            QueryAction queryAction = null;

            queryAction = searchDao.explain(sql);//zhongshu-comment 语法解析，将sql字符串解析为一个Java查询对象

            // TODO add unit tests to explain. (rest level?)
            if (request.path().endsWith("/_explain")) {
                final String jsonExplanation = queryAction.explain().explain();
                return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, jsonExplanation));
            } else {
                Map<String, String> params = request.params();

                //zhongshu-comment 生成一个负责用rest方式查询es的对象RestExecutor
                RestExecutor restExecutor = ActionRequestRestExecuterFactory.createExecutor(params.get("format"));
                final QueryAction finalQueryAction = queryAction;
                //doing this hack because elasticsearch throws exception for un-consumed props
                Map<String, String> additionalParams = new HashMap<>();
                for (String paramName : responseParams()) {
                    if (request.hasParam(paramName)) {
                        additionalParams.put(paramName, request.param(paramName));
                    }
                }

                //zhongshu-comment restExecutor.execute()方法里会调用es查询的相关rest api
                //zhongshu-comment restExecutor.execute()方法的第1、4个参数是框架传进来的参数，第2、3个参数是可以自己生成的参数，所以要多注重一点
                return channel -> restExecutor.execute(client, additionalParams, finalQueryAction, channel);
            }
        } catch (SqlParseException | SQLFeatureNotSupportedException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    protected Set<String> responseParams() {
        Set<String> responseParams = new HashSet<>(super.responseParams());
        responseParams.addAll(Arrays.asList("sql", "flat", "separator", "_score", "_type", "_id", "newLine", "format"));
        return responseParams;
    }
}