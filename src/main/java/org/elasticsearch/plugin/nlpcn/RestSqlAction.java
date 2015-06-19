package org.elasticsearch.plugin.nlpcn;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;
import org.elasticsearch.rest.action.support.RestStatusToXContentListener;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.nlpcn.es4sql.SearchDao;
import org.nlpcn.es4sql.query.explain.ExplainManager;

import java.io.FileOutputStream;

public class RestSqlAction extends BaseRestHandler {

	@Inject
	public RestSqlAction(Settings settings, Client client, RestController restController) {
		super(settings, restController, client);
		restController.registerHandler(RestRequest.Method.POST, "/_sql/_explain", this);
		restController.registerHandler(RestRequest.Method.GET, "/_sql/_explain", this);
		restController.registerHandler(RestRequest.Method.POST, "/_sql", this);
		restController.registerHandler(RestRequest.Method.GET, "/_sql", this);
	}

	@Override
	protected void handleRequest(RestRequest request, RestChannel channel, final Client client) throws Exception {
		final String aggsStart = "<aggs>";
		final String aggsEnd = "</aggs>";


		String sql = request.param("sql");
		String aggs = request.param("aggs");

		if (sql == null) {
			sql = request.content().toUtf8();
			int startIndex = sql.indexOf(aggsStart);
			int endIndex = sql.indexOf(aggsEnd);

			// If <aggs></aggs> is properly included in the Post Body
			if (startIndex >= 0 && endIndex >= 0) {
				// Grab the the aggs element
				String aggElement = sql.substring(startIndex, endIndex + aggsEnd.length());
				// Remove it from the sql param (body)
				sql = sql.replaceAll(aggElement, "");
				// Assign aggs to the value of the element with the the tags
				aggs = aggElement.substring(aggsStart.length(), aggElement.length() - aggsEnd.length());
			}
		}

		SearchDao searchDao = new SearchDao(client);
		ActionRequestBuilder actionRequestBuilder = searchDao.explain(sql);

		ActionRequest actionRequest = actionRequestBuilder.request();

		// Add our aggregations to the action request builder if necessary
		handleAggs(aggs, actionRequestBuilder);

		// TODO add unittests to explain. (rest level?)
		if (request.path().endsWith("/_explain")) {
			String jsonExplanation = ExplainManager.explain(actionRequestBuilder);
			BytesRestResponse bytesRestResponse = new BytesRestResponse(RestStatus.OK, jsonExplanation);
			channel.sendResponse(bytesRestResponse);
		} else {
			new ActionRequestExecuter(actionRequest, channel, client).execute();
		}
	}

	private void handleAggs(String aggs, ActionRequestBuilder actionRequestBuilder) {
		// If the aggs parameter isn't null or empty
		if (aggs != null && aggs.trim().length() > 0) {
			// Get a reference to Action Request Builder
			SearchRequestBuilder b = (SearchRequestBuilder) actionRequestBuilder;

			// Split the comma delimited field names
			String[] arAggs = aggs.split(",");
			for (String strAgg : arAggs) {
				strAgg = strAgg.trim();

				// Split again on colons for sub aggregates
				String[] arSubAggs = strAgg.split(":");
				TermsBuilder objOriginalTerms = null;
				for (String strSubAgg: arSubAggs) {
					if (objOriginalTerms == null) {
						objOriginalTerms = AggregationBuilders.terms(strSubAgg).field(strSubAgg).size(0).order(Terms.Order.term(true));
						// Add our aggregation to the actionRequestBuilder
						b.addAggregation(objOriginalTerms);
					}
					else {
						TermsBuilder objSubTerms = AggregationBuilders.terms(strSubAgg).field(strSubAgg).size(0).order(Terms.Order.term(true));
						objOriginalTerms.subAggregation(objSubTerms);
						objOriginalTerms = objSubTerms;
					}
				}
			}
		}
	}
}