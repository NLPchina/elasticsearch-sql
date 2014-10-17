package org.elasticsearch.plugin.nlpcn;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.support.RestStatusToXContentListener;
import org.nlpcn.es4sql.SearchDao;

public class RestSqlAction extends BaseRestHandler {

	@Inject
	public RestSqlAction(Settings settings, Client client, RestController restController) {
		super(settings, client);
		restController.registerHandler(RestRequest.Method.POST, "/_sql/_explain", this);
		restController.registerHandler(RestRequest.Method.GET, "/_sql/_explain", this);
		restController.registerHandler(RestRequest.Method.POST, "/_sql", this);
		restController.registerHandler(RestRequest.Method.GET, "/_sql", this);
	}

	@Override
	protected void handleRequest(RestRequest request, RestChannel channel, final Client client) throws Exception {

		String sql = request.param("sql");

		if (sql == null) {
			sql = request.content().toUtf8();
		}

		SearchDao searchDao = new SearchDao(client);

		SearchRequestBuilder explan = searchDao.explan(sql);

		if (request.path().endsWith("/_explain")) {
			BytesRestResponse bytesRestResponse = new BytesRestResponse(RestStatus.OK, explan.toString());
			channel.sendResponse(bytesRestResponse);
		} else {
			SearchRequest searchRequest = explan.request();
			searchRequest.listenerThreaded(false);
			client.search(searchRequest, new RestStatusToXContentListener<SearchResponse>(channel));
		}
	}
}
