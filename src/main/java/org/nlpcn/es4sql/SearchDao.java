package org.nlpcn.es4sql;

import java.io.IOException;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.aggregations.Aggregation;
import org.nlpcn.es4sql.domain.SearchResult;
import org.nlpcn.es4sql.domain.Select;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.AggregationQuery;
import org.nlpcn.es4sql.query.DefaultQuery;
import org.nlpcn.es4sql.query.Query;

public class SearchDao {

	private Client client = null;

	@SuppressWarnings("resource")
	public SearchDao(String ip, int port) {
		this.client = new TransportClient().addTransportAddress(new InetSocketTransportAddress(ip, port));
	}

	/**
	 * 查询返回一个初步的封装结果
	 * 
	 * @param sql
	 * @return
	 * @throws IOException
	 * @throws SqlParseException
	 */
	public SearchResult selectAsResult(String sql) throws IOException, SqlParseException {
		Select select = new SqlParser().parseSelect(sql);

		Query query = select2Query(select);

		SearchResponse resp = query.explan().execute().actionGet();

		if (query instanceof DefaultQuery) {
			return new SearchResult(resp);
		} else if (query instanceof AggregationQuery) {
			return new SearchResult(resp,select);
		}
		return null;

	}

	/**
	 * 查询返回es的查询结果
	 * 
	 * @param sql
	 * @return
	 * @throws IOException
	 * @throws SqlParseException
	 */
	public SearchResponse select(String sql) throws IOException, SqlParseException {

		Select select = new SqlParser().parseSelect(sql);

		Query query = select2Query(select);
System.out.println(query.explan());
		return query.explan().execute().actionGet();
	}

	private Query select2Query(Select select) throws SqlParseException {

		Query query = null;

		if (select.isAgg) {
			query = new AggregationQuery(client, select);
		} else {
			query = new DefaultQuery(client, select);
		}
		return query;
	}

}
