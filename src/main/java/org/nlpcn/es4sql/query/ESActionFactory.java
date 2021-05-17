package org.nlpcn.es4sql.query;


import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUnionQuery;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.plugin.nlpcn.ElasticResultHandler;
import org.elasticsearch.plugin.nlpcn.QueryActionElasticExecutor;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.nlpcn.es4sql.domain.Delete;
import org.nlpcn.es4sql.domain.JoinSelect;
import org.nlpcn.es4sql.domain.Select;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.parse.ElasticLexer;
import org.nlpcn.es4sql.parse.ElasticSqlExprParser;
import org.nlpcn.es4sql.parse.SqlParser;
import org.nlpcn.es4sql.parse.SubQueryExpression;
import org.nlpcn.es4sql.query.join.ESJoinQueryActionFactory;
import org.nlpcn.es4sql.query.multi.MultiQueryAction;
import org.nlpcn.es4sql.query.multi.MultiQuerySelect;

import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;

public class ESActionFactory {

	/**
	 * Create the compatible Query object
	 * based on the SQL query.
	 *
	 * @param sql The SQL query.
	 * @return Query object.
	 */
	public static QueryAction create(Client client, String sql) throws SqlParseException, SQLFeatureNotSupportedException {
		sql = sql.replaceAll("\n"," ");
        String firstWord = sql.substring(0, sql.indexOf(' '));
        switch (firstWord.toUpperCase()) {
			case "SELECT":
				SQLQueryExpr sqlExpr = (SQLQueryExpr) toSqlExpr(sql);
                if(isMulti(sqlExpr)){
                    MultiQuerySelect multiSelect = new SqlParser().parseMultiSelect((SQLUnionQuery) sqlExpr.getSubQuery().getQuery());
                    handleSubQueries(client,multiSelect.getFirstSelect());
                    handleSubQueries(client,multiSelect.getSecondSelect());
                    return new MultiQueryAction(client, multiSelect);
                }
                else if(isJoin(sqlExpr,sql)){
                    JoinSelect joinSelect = new SqlParser().parseJoinSelect(sqlExpr);
                    handleSubQueries(client, joinSelect.getFirstTable());
                    handleSubQueries(client, joinSelect.getSecondTable());
                    return ESJoinQueryActionFactory.createJoinAction(client, joinSelect);
                }
                else {
                    Select select = new SqlParser().parseSelect(sqlExpr);
                    handleSubQueries(client, select);
                    return handleSelect(client, select);
                }
			case "DELETE":
                SQLStatementParser parser = createSqlStatementParser(sql);
				SQLDeleteStatement deleteStatement = parser.parseDeleteStatement();
				Delete delete = new SqlParser().parseDelete(deleteStatement);
				return new DeleteQueryAction(client, delete);
            case "SHOW":
                return new ShowQueryAction(client,sql);
			default:
				throw new SQLFeatureNotSupportedException(String.format("Unsupported query: %s", sql));
		}
	}

    private static boolean isMulti(SQLQueryExpr sqlExpr) {
        return sqlExpr.getSubQuery().getQuery() instanceof SQLUnionQuery;
    }

    private static void handleSubQueries(Client client, Select select) throws SqlParseException {
        if (select.containsSubQueries())
        {
            for(SubQueryExpression subQueryExpression : select.getSubQueries()){
                QueryAction queryAction = handleSelect(client, subQueryExpression.getSelect());
                executeAndFillSubQuery(client , subQueryExpression,queryAction);
            }
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

    private static boolean isJoin(SQLQueryExpr sqlExpr, String sql) {
        MySqlSelectQueryBlock query = (MySqlSelectQueryBlock) sqlExpr.getSubQuery().getQuery();
        return query.getFrom() instanceof SQLJoinTableSource && ((SQLJoinTableSource) query.getFrom()).getJoinType() != SQLJoinTableSource.JoinType.COMMA && sql.toLowerCase().contains("join");
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
