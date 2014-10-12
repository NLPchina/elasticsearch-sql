/*
 * Copyright 1999-2011 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.durid.sql;

import java.util.List;

import org.durid.DruidRuntimeException;
import org.durid.sql.ast.SQLExpr;
import org.durid.sql.ast.SQLObject;
import org.durid.sql.ast.SQLStatement;
import org.durid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import org.durid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import org.durid.sql.parser.ParserException;
import org.durid.sql.parser.SQLExprParser;
import org.durid.sql.parser.SQLParseException;
import org.durid.sql.parser.SQLParserUtils;
import org.durid.sql.parser.SQLStatementParser;
import org.durid.sql.parser.Token;
import org.durid.sql.visitor.SQLASTOutputVisitor;
import org.durid.sql.visitor.SchemaStatVisitor;
import org.durid.support.logging.Log;
import org.durid.support.logging.LogFactory;
import org.durid.util.JdbcUtils;

public class SQLUtils {

	private final static Log LOG = LogFactory.getLog(SQLUtils.class);

	public static String toSQLString(SQLObject sqlObject, String dbType) {
		return toMySqlString(sqlObject);
	}

	public static String toSQLString(SQLObject sqlObject) {
		StringBuilder out = new StringBuilder();
		sqlObject.accept(new SQLASTOutputVisitor(out));

		String sql = out.toString();
		return sql;
	}

	public static String toMySqlString(SQLObject sqlObject) {
		StringBuilder out = new StringBuilder();
		sqlObject.accept(new MySqlOutputVisitor(out));

		String sql = out.toString();
		return sql;
	}

	public static SQLExpr toMySqlExpr(String sql) {
		return toSQLExpr(sql, JdbcUtils.MYSQL);
	}

	public static String formatMySql(String sql) {
		return format(sql, JdbcUtils.MYSQL);
	}

	public static String formatOracle(String sql) {
		return format(sql, JdbcUtils.ORACLE);
	}

	public static String formatPGSql(String sql) {
		return format(sql, JdbcUtils.POSTGRESQL);
	}

	public static SQLExpr toSQLExpr(String sql, String dbType) {
		SQLExprParser parser = SQLParserUtils.createExprParser(sql, dbType);
		SQLExpr expr = parser.expr();

		if (parser.getLexer().token() != Token.EOF) {
			throw new ParserException("illegal sql expr : " + sql);
		}

		return expr;
	}

	public static List<SQLStatement> toStatementList(String sql, String dbType) {
		SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, dbType);
		return parser.parseStatementList();
	}

	public static SQLExpr toSQLExpr(String sql) {
		return toSQLExpr(sql, null);
	}

	public static String format(String sql, String dbType) {
		try {
			List<SQLStatement> statementList = toStatementList(sql, dbType);

			StringBuilder out = new StringBuilder();
			SQLASTOutputVisitor visitor = createFormatOutputVisitor(out, statementList, dbType);

			for (SQLStatement stmt : statementList) {
				stmt.accept(visitor);
			}

			return out.toString();
		} catch (SQLParseException ex) {
			LOG.warn("format error", ex);
			return sql;
		} catch (ParserException ex) {
			LOG.warn("format error", ex);
			return sql;
		}
	}

	public static SQLASTOutputVisitor createFormatOutputVisitor(Appendable out, List<SQLStatement> statementList, String dbType) {
		return new MySqlOutputVisitor(out);
	}

	public static SchemaStatVisitor createSchemaStatVisitor(List<SQLStatement> statementList, String dbType) {
		return new MySqlSchemaStatVisitor(); 
	}

	public static List<SQLStatement> parseStatements(String sql, String dbType) {
		SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, dbType);
		List<SQLStatement> stmtList = parser.parseStatementList();
		if (parser.getLexer().token() != Token.EOF) {
			throw new DruidRuntimeException("syntax error : " + sql);
		}
		return stmtList;
	}
}
