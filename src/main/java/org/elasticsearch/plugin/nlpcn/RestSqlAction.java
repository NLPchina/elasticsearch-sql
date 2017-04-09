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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


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
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String sql = request.param("sql");

        if (sql == null) {
            sql = request.content().utf8ToString();
        }
        try {
        SearchDao searchDao = new SearchDao(client);
        QueryAction queryAction= null;

            queryAction = searchDao.explain(sql);

        // TODO add unittests to explain. (rest level?)
        if (request.path().endsWith("/_explain")) {
            final String jsonExplanation = queryAction.explain().explain();
            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, jsonExplanation));
        } else {
            Map<String, String> params = request.params();
            RestExecutor restExecutor = ActionRequestRestExecuterFactory.createExecutor(params.get("format"));
            final QueryAction finalQueryAction = queryAction;
            //doing this hack because elasticsearch throws exception for un-consumed props
            Map<String,String> additionalParams = new HashMap<>();
            List<String> additionalParamsNames = Arrays.asList("_type","_id","_score");
            for(String paramName : additionalParamsNames) {
                additionalParams.put(paramName, request.param(paramName));
            }
            return channel -> restExecutor.execute(client,additionalParams, finalQueryAction,channel);
        }
        } catch (SqlParseException e) {
            e.printStackTrace();
        } catch (SQLFeatureNotSupportedException e) {
            e.printStackTrace();
        }
        return null;

    }

}