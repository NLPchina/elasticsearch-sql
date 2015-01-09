package org.nlpcn.es4sql.query;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.nlpcn.es4sql.domain.Select;
import org.nlpcn.es4sql.exception.SqlParseException;

/**
 * Abstract class. used to transform Select object (Represents SQL query) to
 * SearchRequestBuilder (Represents ES query)
 */
public abstract class Query {

	protected Select select;

	protected Client client;

	protected SearchRequestBuilder request;

	public Query(Client client, Select select) {
		this.client = client;
		this.select = select;
	}

	/**
	 * Prepare the search, and return ES query represention.
	 * @return SearchRequestBuilder (ES query)
	 * @throws SqlParseException
	 */
	public SearchRequestBuilder explain() throws SqlParseException {
		// set index
		request = client.prepareSearch(select.getIndexArr());

		// set type
		String[] typeArr = select.getTypeArr();
		if (typeArr != null) {
			request.setTypes(typeArr);
		}

		return _explain();
	}

	/**
	 * Make the actual Select to ES query transofrmation.
	 * @return ES query.
	 * @throws SqlParseException
	 */
	protected abstract SearchRequestBuilder _explain() throws SqlParseException;

}
