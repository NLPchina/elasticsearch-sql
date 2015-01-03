package org.nlpcn.es4sql;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.durid.sql.SQLUtils;
import org.durid.sql.ast.expr.SQLQueryExpr;
import org.durid.sql.ast.statement.SQLTableSource;
import org.durid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
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
import org.nlpcn.es4sql.query.QueryFactory;


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
	 * Prepare Search And transform sql
	 * into ES Search Request
	 * @param sql SQL query to execute.
	 * @return ES search request
	 * @throws SqlParseException
	 */
	public SearchRequestBuilder explain(String sql) throws SqlParseException {
		SQLQueryExpr sqlExpr = toSqlExpr(sql);

		Select select = new SqlParser().parseSelect(sqlExpr);
		Query query = QueryFactory.create(client, select);
		return query.explain();
	}


	/**
	 * 对table进行特殊处理
	 * 
	 * @param sql
	 * @return
	 */
	private SQLQueryExpr toSqlExpr(String sql) {

		String[] split = sql.split("\\s+");

		StringBuilder sb = new StringBuilder();
		StringBuilder sbRegex = new StringBuilder();

		boolean beginTable = false;

		for (String str : split) {

			if (!beginTable) {
				if ("from".equals(str.toLowerCase().trim())) {
					beginTable = true;
				}
				continue;
			}

			if (END_TABLE_MAP.contains(str.trim().toLowerCase())) {
				break;
			}
			sb.append(str);
			sbRegex.append(str);
			sbRegex.append("\\s+");
		}

		sql = sql.replaceFirst(sbRegex.toString(), "TEMP_TABLE ");

		SQLQueryExpr expr = (SQLQueryExpr) SQLUtils.toMySqlExpr(sql);

		MySqlSelectQueryBlock query = (MySqlSelectQueryBlock) expr.getSubQuery().getQuery();

		SQLTableSource from = query.getFrom();

		from.setAlias(sb.toString());

		return expr;
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

}
