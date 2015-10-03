package org.nlpcn.es4sql.query;


import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.*;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.plugin.nlpcn.ElasticResultHandler;
import org.elasticsearch.plugin.nlpcn.QueryActionElasticExecutor;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.nlpcn.es4sql.domain.Delete;
import org.nlpcn.es4sql.domain.JoinSelect;
import org.nlpcn.es4sql.domain.Select;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.parse.ElasticLexer;
import org.nlpcn.es4sql.parse.ElasticSqlExprParser;
import org.nlpcn.es4sql.parse.SqlParser;
import org.nlpcn.es4sql.parse.SubQueryExpression;
import org.nlpcn.es4sql.query.join.ESJoinQueryActionFactory;

import java.io.IOException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

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
                if(isJoin(sqlExpr,sql)){
                    JoinSelect joinSelect = new SqlParser().parseJoinSelect(sqlExpr);
                    return ESJoinQueryActionFactory.createJoinAction(client, joinSelect);
                }
                else {
                    Select select = new SqlParser().parseSelect(sqlExpr);

                    if (select.containsSubQueries())
                    {
                        for(SubQueryExpression subQueryExpression : select.getSubQueries()){
                            QueryAction queryAction = handleSelect(client, subQueryExpression.getSelect());
                            executeAndFillSubQuery(client , subQueryExpression,queryAction);
                        }
                    }
                    return handleSelect(client, select);
                }
			case "DELETE":
                SQLStatementParser parser = createSqlStatementParser(sql);
				SQLDeleteStatement deleteStatement = parser.parseDeleteStatement();
				Delete delete = new SqlParser().parseDelete(deleteStatement);
				return new DeleteQueryAction(client, delete);

			default:
				throw new SQLFeatureNotSupportedException(String.format("Unsupported query: %s", sql));
		}
	}

    private static void executeAndFillSubQuery(Client client , SubQueryExpression subQueryExpression,QueryAction queryAction) throws SqlParseException {
        List<Object> values = new ArrayList<>();
        Object queryResult;
        try {
            queryResult = QueryActionElasticExecutor.executeAnyAction(client,queryAction);
        } catch (Exception e) {
            throw new SqlParseException("could not execute SubQuery: " +  e.getMessage());
        }

        String returnField = subQueryExpression.getReturnField();
        if(queryResult instanceof SearchHits) {
            SearchHits hits = (SearchHits) queryResult;
            for (SearchHit hit : hits) {
                values.add(ElasticResultHandler.getFieldValue(hit,returnField));
            }
        }
        else {
            throw new SqlParseException("on sub queries only support queries that return Hits and not aggregations");
        }
        subQueryExpression.setValues(values.toArray());
    }

    private static QueryAction handleSelect(Client client, Select select) {
        if (select.isAgg) {
            return new AggregationQueryAction(client, select);
        } else {
            return new DefaultQueryAction(client, select);
        }
    }

    private static SQLStatementParser createSqlStatementParser(String sql) {
        ElasticLexer lexer = new ElasticLexer(sql);
        lexer.nextToken();
        return new MySqlStatementParser(lexer);
    }

    private static boolean isJoin(SQLQueryExpr sqlExpr,String sql) {
        MySqlSelectQueryBlock query = (MySqlSelectQueryBlock) sqlExpr.getSubQuery().getQuery();
        return query.getFrom() instanceof  SQLJoinTableSource && sql.toLowerCase().contains("join");
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
