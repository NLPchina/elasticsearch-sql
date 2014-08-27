package org.nlpcn.es4sql;

import java.util.ArrayList;
import java.util.List;

import org.nlpcn.es4sql.domain.Condition;
import org.nlpcn.es4sql.domain.Field;
import org.nlpcn.es4sql.domain.MethodField;
import org.nlpcn.es4sql.domain.Select;
import org.nlpcn.es4sql.domain.Where;
import org.nlpcn.es4sql.domain.Where.CONN;
import org.nlpcn.es4sql.exception.SqlParseException;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLBetweenExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumericLiteralExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectGroupByClause;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock.Limit;

/**
 * es sql support
 * 
 * @author ansj
 *
 */
public class SqlParser {

	public SqlParser() {
	};

	public Select parseSelect(String sql) throws SqlParseException {

		SQLQueryExpr mySqlExpr = (SQLQueryExpr) SQLUtils.toMySqlExpr(sql);

		MySqlSelectQueryBlock query = (MySqlSelectQueryBlock) mySqlExpr.getSubQuery().getQuery();

		Select select = new Select();

		findSelect(query, select);

		findFrom(query, select);

		findWhere(query, select);

		findLimit(query, select);

		findOrderBy(query, select);

		findGroupBy(query, select);

		return select;
	}

	private void findWhere(MySqlSelectQueryBlock query, Select select) throws SqlParseException {
		SQLExpr where = query.getWhere();
		if (where == null) {
			return;
		}

		Where myWhere = Where.newInstance();
		parseWhere(where, myWhere);
		select.setWhere(myWhere);
	}

	private boolean isCond(SQLBinaryOpExpr expr) {
		return expr.getLeft() instanceof SQLIdentifierExpr;
	}

	private void parseWhere(SQLExpr expr, Where where) throws SqlParseException {
		if (expr instanceof SQLBinaryOpExpr && !isCond((SQLBinaryOpExpr) expr)) {
			SQLBinaryOpExpr bExpr = (SQLBinaryOpExpr) expr;
			routeCond(bExpr, bExpr.left, where);
			routeCond(bExpr, bExpr.right, where);
		} else {
			explanCond("AND", expr, where);
		}
	}

	private void routeCond(SQLBinaryOpExpr bExpr, SQLExpr sub, Where where) throws SqlParseException {
		if (sub instanceof SQLBinaryOpExpr) {
			parseWhere(bExpr, (SQLBinaryOpExpr) sub, where);
		} else if (sub instanceof SQLInListExpr) {
			explanCond(bExpr.operator.name, sub, where);
		} else {
			throw new SqlParseException("error class type sub :" + sub.getClass());
		}
	}

	private void parseWhere(SQLBinaryOpExpr expr, SQLBinaryOpExpr sub, Where where) throws SqlParseException {
		if (isCond(sub)) {
			explanCond(expr.operator.name, sub, where);
		} else {
			if (sub.operator.priority < expr.operator.priority) {
				Where subWhere = new Where(expr.getOperator().name);
				where.addWhere(subWhere);
				parseWhere(sub, subWhere);
			} else {
				parseWhere(sub, where);
			}
		}

	}

	private void explanCond(String opear, SQLExpr expr, Where where) throws SqlParseException {
		if (expr instanceof SQLBinaryOpExpr) {
			SQLBinaryOpExpr soExpr = (SQLBinaryOpExpr) expr;
			Condition condition = new Condition(CONN.valueOf(opear), soExpr.getLeft().toString(), soExpr.getOperator().name, parseValue(soExpr.getRight()));
			where.addWhere(condition);
		} else if (expr instanceof SQLInListExpr) {
			SQLInListExpr siExpr = (SQLInListExpr) expr;
			Condition condition = new Condition(CONN.valueOf(opear), siExpr.getExpr().toString(), siExpr.isNot()?"NOT IN":"IN", parseValue(siExpr.getTargetList()));
			where.addWhere(condition);
		} else if (expr instanceof SQLBetweenExpr) {
			SQLBetweenExpr between = ((SQLBetweenExpr) expr) ;
			Condition condition = new Condition(CONN.valueOf(opear), between.getTestExpr().toString(),between.isNot()?"NOT BETWEEN":"BETWEEN" , new Object[]{parseValue(between.beginExpr),parseValue(between.endExpr)});
			where.addWhere(condition);
		} else {
			throw new SqlParseException("err find condition " + expr.getClass());
		}
	}

	private Object[] parseValue(List<SQLExpr> targetList) throws SqlParseException {
		Object[] value = new Object[targetList.size()];
		for (int i = 0; i < targetList.size(); i++) {
			value[i] = parseValue(targetList.get(i));
		}
		return value;
	}

	private Object parseValue(SQLExpr expr) throws SqlParseException {
		if (expr instanceof SQLNumericLiteralExpr) {
			return ((SQLNumericLiteralExpr) expr).getNumber();
		} else if (expr instanceof SQLCharExpr) {
			return ((SQLCharExpr) expr).getText();
		} else if (expr instanceof SQLMethodInvokeExpr) {
			return expr;
		} else if (expr instanceof SQLNullExpr) {
			return null;
		} else {
			throw new SqlParseException("i can not know value type " + expr.getClass() + " , value is : " + expr);
		}
	}

	private void findSelect(MySqlSelectQueryBlock query, Select select) throws SqlParseException {
		List<SQLSelectItem> selectList = query.getSelectList();

		SQLExpr expr = null;
		for (SQLSelectItem sqlSelectItem : selectList) {
			expr = sqlSelectItem.getExpr();
			if (expr instanceof SQLIdentifierExpr) {
				select.addField(expr.toString(), sqlSelectItem.getAlias());
			} else if (expr instanceof SQLQueryExpr) {
				throw new SqlParseException("unknow field name : " + sqlSelectItem);
			} else if (expr instanceof SQLAllColumnExpr) {
			} else if (expr instanceof SQLMethodInvokeExpr) {
				select.addField(((SQLMethodInvokeExpr) expr).getMethodName(), ((SQLMethodInvokeExpr) expr).getParameters(), sqlSelectItem.getAlias());
			} else if (expr instanceof SQLAggregateExpr) {
				select.addField(((SQLAggregateExpr) expr).getMethodName(), ((SQLAggregateExpr) expr).getArguments(), ((SQLAggregateExpr) expr).getOption(), sqlSelectItem.getAlias());
			} else {
				throw new SqlParseException("unknow field name : " + sqlSelectItem);
			}
		}

	}

	private void findGroupBy(MySqlSelectQueryBlock query, Select select) {
		SQLSelectGroupByClause groupBy = query.getGroupBy();
		if (groupBy == null) {
			return;
		}
		List<SQLExpr> items = groupBy.getItems();

		for (SQLExpr sqlExpr : items) {
			select.addGroupBy(sqlExpr.toString());
		}
	}

	private void findOrderBy(MySqlSelectQueryBlock query, Select select) throws SqlParseException {
		SQLOrderBy orderBy = query.getOrderBy();

		if (orderBy == null) {
			return;
		}

		List<SQLSelectOrderByItem> items = orderBy.getItems();

		List<String> lists = new ArrayList<>();

		for (SQLSelectOrderByItem sqlSelectOrderByItem : items) {

			SQLExpr expr = sqlSelectOrderByItem.getExpr();
			if (expr instanceof SQLIdentifierExpr) {
				lists.add(expr.toString());
			} else if (expr instanceof SQLMethodInvokeExpr) {
				SQLMethodInvokeExpr aggExpr = ((SQLMethodInvokeExpr) expr);
				lists.add(MethodField.makeField(aggExpr.getMethodName(), aggExpr.getParameters(), null, null).toString());
			} else if (expr instanceof SQLAggregateExpr) {
				SQLAggregateExpr aggExpr = ((SQLAggregateExpr) expr);
				lists.add(MethodField.makeField(aggExpr.getMethodName(), aggExpr.getArguments(), ((SQLAggregateExpr) expr).getOption() == null ? null : ((SQLAggregateExpr) expr).getOption().name(),
						null).toString());
			} else {
				throw new SqlParseException("unknow order by name : " + expr);
			}

			if (sqlSelectOrderByItem.getType() != null) {
				String type = sqlSelectOrderByItem.getType().toString();
				for (String name : lists) {
					select.addOrderBy(name, type);
				}
				lists.clear();
			}
		}

	}

	private void findLimit(MySqlSelectQueryBlock query, Select select) {
		Limit limit = query.getLimit();

		if (limit == null) {
			return;
		}

		select.setRowCount(Integer.parseInt(limit.getRowCount().toString()));

		if (limit.getOffset() != null)
			select.setOffset(Integer.parseInt(limit.getOffset().toString()));
	}

	private void findFrom(MySqlSelectQueryBlock query, Select select) {
		SQLTableSource from = query.getFrom();

		String[] split = from.toString().split(",");

		for (String string : split) {
			select.addIndexAndType(string.trim());
		}
	}

}
