package org.nlpcn.es4sql.query;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.nlpcn.es4sql.domain.Query;
import org.nlpcn.es4sql.domain.Select;
import org.nlpcn.es4sql.domain.hints.Hint;
import org.nlpcn.es4sql.domain.hints.HintType;
import org.nlpcn.es4sql.exception.SqlParseException;

import java.util.ArrayList;

/**
 * Abstract class. used to transform Select object (Represents SQL query) to
 * SearchRequestBuilder (Represents ES query)
 */
public abstract class QueryAction {

	protected org.nlpcn.es4sql.domain.Query query;

	protected Client client;


	public QueryAction(Client client, Query query) {
		this.client = client;
		this.query = query;
	}

    protected void updateRequestWithIndexAndRoutingOptions(Select select, SearchRequestBuilder request) {
        for(Hint hint : select.getHints()){
            if(hint.getType() == HintType.IGNORE_UNAVAILABLE){
                //saving the defaults from TransportClient search
                request.setIndicesOptions(IndicesOptions.fromOptions(true, false, true, false, IndicesOptions.strictExpandOpenAndForbidClosed()));
            }
            if(hint.getType() == HintType.ROUTINGS){
                Object[] routings = hint.getParams();
                String[] routingsAsStringArray = new String[routings.length];
                for(int i=0;i<routings.length;i++){
                    routingsAsStringArray[i]=routings[i].toString();
                }
                request.setRouting(routingsAsStringArray);
            }
        }
    }



	/**
	 * Prepare the request, and return ES request.
	 * @return ActionRequestBuilder (ES request)
	 * @throws SqlParseException
	 */
	public abstract SqlElasticRequestBuilder explain() throws SqlParseException;

}
