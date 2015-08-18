package org.nlpcn.es4sql.query;


import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.alibaba.druid.sql.parser.*;
import com.alibaba.druid.util.JdbcUtils;
import org.elasticsearch.client.Client;
import org.nlpcn.es4sql.domain.Delete;
import org.nlpcn.es4sql.domain.Select;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.parse.ElasticSqlExprParser;
import org.nlpcn.es4sql.parse.SqlParser;

import java.sql.SQLFeatureNotSupportedException;

public class ESActionFactory {

	/**
	 * Create the compatible Query object
	 * based on the SQL query.
	 *
	 * @param sql The SQL query.
	 * @return Query object.
	 */
	public static QueryAction create(Client client, String sql) throws SqlParseException, SQLFeatureNotSupportedException {
		String firstWord = sql.substring(0, sql.indexOf(' '));
		switch (firstWord.toUpperCase()) {
			case "SELECT":
				SQLQueryExpr sqlExpr = (SQLQueryExpr) toSqlExpr(sql);

				Select select = new SqlParser().parseSelect(sqlExpr);

				if (select.isAgg) {
					return new AggregationQueryAction(client, select);
				} else {
					return new DefaultQueryAction(client, select);
				}
			case "DELETE":
				SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, JdbcUtils.MYSQL);
				SQLDeleteStatement deleteStatement = parser.parseDeleteStatement();
				Delete delete = new SqlParser().parseDelete(deleteStatement);
				return new DeleteQueryAction(client, delete);

			default:
				throw new SQLFeatureNotSupportedException(String.format("Unsupported query: %s", sql));
		}
	}

    private static SQLExpr toSqlExpr(String sql) {
        SQLExprParser parser = new ElasticSqlExprParser(sql);
        SQLExpr expr = parser.expr();

        if (parser.getLexer().token() != Token.EOF) {
            throw new ParserException("illegal sql expr : " + sql);
        }

        return expr;
    }
}
