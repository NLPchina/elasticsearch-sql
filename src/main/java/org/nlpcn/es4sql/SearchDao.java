package org.nlpcn.es4sql;

import java.sql.SQLFeatureNotSupportedException;
import java.util.HashSet;
import java.util.Set;

import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.Client;
import org.nlpcn.es4sql.domain.Delete;
import org.nlpcn.es4sql.domain.JoinSelect;
import org.nlpcn.es4sql.domain.Select;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.parse.SqlParser;
import org.nlpcn.es4sql.query.*;
import org.nlpcn.es4sql.query.join.ESJoinQueryActionFactory;


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

    public Client getClient() {
        return client;
    }

    /**
	 * Prepare action And transform sql
	 * into ES ActionRequest
	 * @param sql SQL query to execute.
	 * @return ES request
	 * @throws SqlParseException
	 */
	public QueryAction explain(String sql) throws SqlParseException, SQLFeatureNotSupportedException {
		return ESActionFactory.create(client, sql);
	}

	public String getTableName(String sql) throws SqlParseException, SQLFeatureNotSupportedException {
		return ESActionFactory.table(client, sql);
	}



}
