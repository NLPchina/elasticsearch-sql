package org.elasticsearch.plugin.nlpcn;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.action.support.RestStatusToXContentListener;


public class ActionRequestExecuter {

	private RestChannel channel;
	private Client client;
	private ActionRequest request;

	public ActionRequestExecuter(ActionRequest request, RestChannel channel, final Client client) {
		this.request = request;
		this.channel = channel;
		this.client = client;
	}

	/**
	 * Execute the ActionRequest and returns the REST response using the channel.
	 */
	public void execute() throws Exception {
		request.listenerThreaded(false);

		if (request instanceof SearchRequest) {
			client.search((SearchRequest) request, new RestStatusToXContentListener<SearchResponse>(channel));
		} else if (request instanceof DeleteByQueryRequest) {
			client.deleteByQuery((DeleteByQueryRequest) request, new DeleteByQueryRestListener(channel));
		}
		else {
			throw new Exception(String.format("Unsupported ActionRequest provided: %s", request.getClass().getName()));
		}
	}

}
