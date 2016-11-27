package org.elasticsearch.plugin.nlpcn;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugin.nlpcn.executors.ActionRequestRestExecuterFactory;
import org.elasticsearch.plugin.nlpcn.executors.RestExecutor;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.nlpcn.es4sql.SearchDao;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.QueryAction;
import org.nlpcn.es4sql.query.SqlElasticRequestBuilder;

import java.io.IOException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;


public class RestSqlAction extends BaseRestHandler {

//    public static final RestSqlAction INSTANCE = new RestSqlAction();


    @Inject
	public RestSqlAction(Settings settings, RestController restController) {
        super(settings);
		restController.registerHandler(RestRequest.Method.POST, "/_sql/_explain", this);
		restController.registerHandler(RestRequest.Method.GET, "/_sql/_explain", this);
		restController.registerHandler(RestRequest.Method.POST, "/_sql", this);
		restController.registerHandler(RestRequest.Method.GET, "/_sql", this);
	}

//	@Override
//	public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
//		String sql = request.param("sql");
//
//		if (sql == null) {
//			sql = request.content().utf8ToString();
//		}
//		SearchDao searchDao = new SearchDao(client);
//        QueryAction queryAction= searchDao.explain(sql);
//
//		// TODO add unittests to explain. (rest level?)
//		if (request.path().endsWith("/_explain")) {
//			String jsonExplanation = queryAction.explain().explain();
//			BytesRestResponse bytesRestResponse = new BytesRestResponse(RestStatus.OK, jsonExplanation);
//			channel.sendResponse(bytesRestResponse);
//		} else {
//            Map<String, String> params = request.params();
//            RestExecutor restExecutor = ActionRequestRestExecuterFactory.createExecutor(params.get("format"));
//			restExecutor.execute(client,params,queryAction,channel);
//		}
//	}

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
            return channel -> restExecutor.execute(client,params, finalQueryAction,channel);
        }
        } catch (SqlParseException e) {
            e.printStackTrace();
        } catch (SQLFeatureNotSupportedException e) {
            e.printStackTrace();
        }
        return null;

    }

}