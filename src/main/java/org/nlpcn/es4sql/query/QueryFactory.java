package org.nlpcn.es4sql.query;


import org.elasticsearch.client.Client;
import org.nlpcn.es4sql.domain.Select;

public class QueryFactory {

	/**
	 * Create the compatible Query object
	 * based on the SQL Select query structre.
	 *
	 * @param select The SQL query.
	 * @return Query object.
	 */
	public static Query create(Client client, Select select) {
		if (select.isAgg) {
			return new AggregationQuery(client, select);
		} else {
			return new DefaultQuery(client, select);
		}
	}
}
