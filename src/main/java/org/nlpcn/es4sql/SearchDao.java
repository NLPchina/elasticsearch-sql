package org.nlpcn.es4sql;

import java.io.IOException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.nlpcn.es4sql.domain.Select;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.parse.SqlParser;
import org.nlpcn.es4sql.query.AggregationQuery;
import org.nlpcn.es4sql.query.DefaultQuery;
import org.nlpcn.es4sql.query.Query;

import org.durid.sql.SQLUtils;
import org.durid.sql.ast.expr.SQLQueryExpr;

public class SearchDao {

	private Client client = null;

	@SuppressWarnings("resource")
	public SearchDao(String ip, int port) {
		this.client = new TransportClient().addTransportAddress(new InetSocketTransportAddress(ip, port));
	}

	public SearchDao(Client client) {
		this.client = client;
	}

	@SuppressWarnings("resource")
	public SearchDao(String clusterName, String ip, int port) {
		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build();
		this.client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(ip, port));
	}

	private SearchRequestBuilder explan(SQLQueryExpr SQLQueryExpr) throws SqlParseException {
		Select select = new SqlParser().parseSelect(SQLQueryExpr);

		Query query = null;

		Client client = new TransportClient();

		if (select.isAgg) {
			query = new AggregationQuery(client, select);
		} else {
			query = new DefaultQuery(client, select);
		}
		return query.explan();
	}

	/**
	 * 把sql解析成es的查询
	 * 
	 * @param sql
	 * @return
	 * @throws SqlParseException
	 */
	public SearchRequestBuilder explan(String sql) throws SqlParseException {
		return explan((SQLQueryExpr) SQLUtils.toMySqlExpr(sql));
	}

	/**
	 * 執行一條sql
	 * 
	 * @param sql
	 * @return
	 * @throws IOException
	 * @throws SqlParseException
	 */
	public SearchResponse execute(String sql) throws IOException, SqlParseException {
		return select((SQLQueryExpr) SQLUtils.toMySqlExpr(sql));
	}

	/**
	 * 刪除一個索引
	 * 
	 * @param index
	 * @return
	 * @throws SqlParseException
	 */
	public ActionResponse drop(String index) throws SqlParseException {
		DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(index);
		ActionFuture<DeleteIndexResponse> delete = client.admin().indices().delete(deleteIndexRequest);
		return delete.actionGet();
	}

	/**
	 * 查询返回es的查询结果
	 * 
	 * @param sql
	 * @return
	 * @throws IOException
	 * @throws SqlParseException
	 */
	private SearchResponse select(SQLQueryExpr mySqlExpr) throws IOException, SqlParseException {

		Select select = new SqlParser().parseSelect(mySqlExpr);

		Query query = select2Query(select);

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

	public Client getClient() {
		return client;
	}
}
