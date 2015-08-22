package org.nlpcn.es4sql.query;


import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.parser.*;
import com.alibaba.druid.util.JdbcUtils;
import org.elasticsearch.client.Client;
import org.nlpcn.es4sql.domain.Delete;
import org.nlpcn.es4sql.domain.JoinSelect;
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
                if(isJoin(sqlExpr,sql)){
                    JoinSelect joinSelect = new SqlParser().parseJoinSelect(sqlExpr);
                    return new ESHashJoinQueryAction(client,joinSelect);
                    //NestedLoopQueryAction(client)
                    //Join  between two tables : s1 and s2
                    // Query contains:
                    // fields: s1 connected fields , s2 connected fields , s1 fields for query
                    // s1 = select  from SqlParser for one of them (need to take only first table wheres)
                    // c = conditions for crossed -> each condition is field = s2Field , value = s1Field
                    // s2 = select from SqlParser for the 2nd (need to take only 2nd table wheres)

                    //NestedLoopsStrategy
                    // choose arbitrary one of them
                    // res1 = DefaultQueryAction(client, s1 -> add connectedFields to selected fields);
                    // for each r1 : res1
                        // duplicate s2 ;  s2.conditions += c(replace value to r1 values)
                        // res2_r1 = DefaultQueryAction(client,s2)
                        // foreach r2: res2
                            //  results_set+= union(r2.fields,r1.only_return_fields)

                    //HashJoinStrategy - only when all equals
                    //FirstStrategy
                    //Choose arbitrary one of them
                    //res1 = DefaultQueryAction(client, s1 -> add connectedFields to selected fields);
                    // create HashMap<String,document>
                    // foreach r1 : res1
                        // str1 = String.concat(connectedFields.getValues(r1))
                        // add str1,r1 to hashMap
                    // res2 = DefaultQueryAction(client, s2 -> add connectedFields to selected fields)
                    //foreach r2 : res2
                        // str2 = String.concat(connectedFields.getValues(r1))
                        // if map.contains(str2)
                            // results_set += union(r2.only_return_fields , r1.only_return_fields)

                    //SecondStrategy
                    //res1 = DefaultQueryAction(client, s1 -> add connectedFields to selected fields);
                    // create dict = HashMap<String,document>
                    // create filters =  HashMap<String,List<Object>>
                    // foreach r1 : res1
                        // str1 = String.concat(connectedFields.getValues(r1))
                        // add str1,r1 to hashMap
                        // foreach cond : c
                            //filters[c.field2Name]+=c.value(r,field1Name)
                    //foreach f : filters
                        // s2.addFilter terms/in Filter (f.value)
                    // res2 = DefaultQueryAction(client, s2 -> add connectedFields to selected fields)
                    //foreach r2 : res2
                    // str2 = String.concat(connectedFields.getValues(r1))
                    // if map.contains(str2)
                    // results_set += union(r2.only_return_fields , r1.only_return_fields)

                }
                else {
                    Select select = new SqlParser().parseSelect(sqlExpr);

                    if (select.isAgg) {
                        return new AggregationQueryAction(client, select);
                    } else {
                        return new DefaultQueryAction(client, select);
                    }
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
