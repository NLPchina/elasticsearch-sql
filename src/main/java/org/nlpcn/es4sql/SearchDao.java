package org.nlpcn.es4sql;

import java.sql.SQLFeatureNotSupportedException;
import java.util.HashSet;
import java.util.Set;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.Client;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.ESActionFactory;
import org.nlpcn.es4sql.query.QueryAction;


public class SearchDao {

	private static final Set<String> END_TABLE_MAP = new HashSet<>();

	static {
		END_TABLE_MAP.add("limit");
		END_TABLE_MAP.add("order");
		END_TABLE_MAP.add("where");
		END_TABLE_MAP.add("group");

	}

	private Client client = null;


	public SearchDao(Client client) {
		this.client = client;
	}


	/**
	 * Prepare action And transform sql
	 * into ES ActionRequest
	 * @param sql SQL query to execute.
	 * @return ES request
	 * @throws SqlParseException
	 */
	public ActionRequestBuilder explain(String sql) throws SqlParseException, SQLFeatureNotSupportedException {

		QueryAction query = ESActionFactory.create(client, sql);
		return query.explain();
	}

}
