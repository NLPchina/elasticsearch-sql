package org.elasticsearch.plugin.nlpcn;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.action.support.RestStatusToXContentListener;
import org.nlpcn.es4sql.query.ESHashJoinQueryAction;
import org.nlpcn.es4sql.query.HashJoinElasticRequestBuilder;
import org.nlpcn.es4sql.query.SqlElasticRequestBuilder;


public class ActionRequestExecuter {

	private RestChannel channel;
	private Client client;
	private SqlElasticRequestBuilder requestBuilder;

	public ActionRequestExecuter(SqlElasticRequestBuilder requestBuilder, RestChannel channel, final Client client) {
		this.requestBuilder = requestBuilder;
		this.channel = channel;
		this.client = client;
	}

	/**
	 * Execute the ActionRequest and returns the REST response using the channel.
	 */
	public void execute() throws Exception {
        ActionRequest request = requestBuilder.request();
        request.listenerThreaded(false);

        //todo: maby change to instanceof multi?
        if(requestBuilder instanceof HashJoinElasticRequestBuilder){
            HashJoinElasticRequestBuilder hashJoin = (HashJoinElasticRequestBuilder) requestBuilder;
            HashJoinElasticExecutor executor = new HashJoinElasticExecutor(client,hashJoin);
            executor.run();
            executor.sendResponse(channel);
        }
		else if (request instanceof SearchRequest) {
			client.search((SearchRequest) request, new RestStatusToXContentListener<SearchResponse>(channel));
		} else if (request instanceof DeleteByQueryRequest) {
			client.deleteByQuery((DeleteByQueryRequest) request, new DeleteByQueryRestListener(channel));
		}


		else {
			throw new Exception(String.format("Unsupported ActionRequest provided: %s", request.getClass().getName()));
		}
	}

}
