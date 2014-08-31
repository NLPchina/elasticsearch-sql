package org.elasticsearch.plugin.nlpcn;

import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.support.RestBuilderListener;
import org.elasticsearch.rest.action.support.RestStatusToXContentListener;
import org.nlpcn.es4sql.domain.Select;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.parse.SqlParser;
import org.nlpcn.es4sql.query.AggregationQuery;
import org.nlpcn.es4sql.query.DefaultQuery;
import org.nlpcn.es4sql.query.Query;

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

		final String sql = request.param("sql");

		if (request.path().endsWith("/_explain")) {

			Select select = new SqlParser().parseSelect(sql);
			SearchRequestBuilder explan = select2Query(select, client).explan();
			BytesRestResponse bytesRestResponse = new BytesRestResponse(RestStatus.OK, explan.toString());
			channel.sendResponse(bytesRestResponse);
		} else {
			Select select = new SqlParser().parseSelect(sql);
			SearchRequest searchRequest = select2Query(select, client).explan().request();
			searchRequest.listenerThreaded(false);
			client.search(searchRequest, new RestStatusToXContentListener<SearchResponse>(channel));
		}
	}

	private Query select2Query(Select select, Client client) throws SqlParseException {

		Query query = null;

		if (select.isAgg) {
			query = new AggregationQuery(client, select);
		} else {
			query = new DefaultQuery(client, select);
		}
		return query;
	}
}
