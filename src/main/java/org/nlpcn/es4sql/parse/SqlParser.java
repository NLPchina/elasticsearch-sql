package org.nlpcn.es4sql.parse;

import java.util.*;

import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlSelectGroupByExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;


import org.nlpcn.es4sql.domain.*;
import org.nlpcn.es4sql.domain.Where.CONN;
import org.nlpcn.es4sql.domain.hints.Hint;
import org.nlpcn.es4sql.domain.hints.HintFactory;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.spatial.SpatialParamsFactory;

/**
 * es sql support
 * 
 * @author ansj
 * 
 */
public class SqlParser {

	public SqlParser() {
	};

	public Select parseSelect(SQLQueryExpr mySqlExpr) throws SqlParseException {

		MySqlSelectQueryBlock query = (MySqlSelectQueryBlock) mySqlExpr.getSubQuery().getQuery();

		Select select = new Select();

		findSelect(query, select,null);

		select.getFrom().addAll(findFrom(query.getFrom()));

		select.setWhere(findWhere(query.getWhere()));

		findLimit(query.getLimit(), select);

		findOrderBy(query, select);

		findGroupBy(query, select);

		return select;
	}


	public Delete parseDelete(SQLDeleteStatement deleteStatement) throws SqlParseException {
		Delete delete = new Delete();

		delete.getFrom().addAll(findFrom(deleteStatement.getTableSource()));

		delete.setWhere(findWhere(deleteStatement.getWhere()));

		return delete;
	}

	private Where findWhere(SQLExpr where) throws SqlParseException {
		if(where == null) {
			return null;
		}

		Where myWhere = Where.newInstance();
		parseWhere(where, myWhere);
		return myWhere;
	}


	private boolean isCond(SQLBinaryOpExpr expr) {
		return expr.getLeft() instanceof SQLIdentifierExpr || expr.getLeft() instanceof SQLPropertyExpr;
	}

	private void parseWhere(SQLExpr expr, Where where) throws SqlParseException {
		if (expr instanceof SQLBinaryOpExpr && !isCond((SQLBinaryOpExpr) expr)) {
			SQLBinaryOpExpr bExpr = (SQLBinaryOpExpr) expr;
			routeCond(bExpr, bExpr.getLeft(), where);
			routeCond(bExpr, bExpr.getRight(), where);
		} else {
			explanCond("AND", expr, where);
		}
	}

	private void routeCond(SQLBinaryOpExpr bExpr, SQLExpr sub, Where where) throws SqlParseException {
		if (sub instanceof SQLBinaryOpExpr) {
			parseWhere(bExpr, (SQLBinaryOpExpr) sub, where);
		} else {
			explanCond(bExpr.getOperator().name, sub, where);
		}
	}

	private void parseWhere(SQLBinaryOpExpr expr, SQLBinaryOpExpr sub, Where where) throws SqlParseException {
		if (isCond(sub)) {
			explanCond(expr.getOperator().name, sub, where);
		} else {
			if (sub.getOperator().priority != expr.getOperator().priority) {
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
            Condition condition = new Condition(CONN.valueOf(opear), siExpr.getExpr().toString(), siExpr.isNot() ? "NOT IN" : "IN", parseValue(siExpr.getTargetList()));
            where.addWhere(condition);
        } else if (expr instanceof SQLBetweenExpr) {
            SQLBetweenExpr between = ((SQLBetweenExpr) expr);
            Condition condition = new Condition(CONN.valueOf(opear), between.getTestExpr().toString(), between.isNot() ? "NOT BETWEEN" : "BETWEEN", new Object[]{parseValue(between.beginExpr),
                    parseValue(between.endExpr)});
            where.addWhere(condition);
        } else if (expr instanceof SQLNotExpr){
            SQLBinaryOpExpr notExpr = (SQLBinaryOpExpr) ((SQLNotExpr) expr).getExpr();
            String left = notExpr.getLeft().toString();
            SQLExpr right = notExpr.getRight();
            // add a check here to see if the not'd value is a 'like' operator
            Condition.OPEAR notOpear = notExpr.getOperator() == SQLBinaryOperator.Like ? Condition.OPEAR.NLIKE : Condition.OPEAR.N;
            Condition condition = new Condition(CONN.valueOf(opear), left, notOpear, parseValue(right));
            where.addWhere(condition);
        }
        else if (expr instanceof SQLMethodInvokeExpr) {
            SQLMethodInvokeExpr methodExpr = (SQLMethodInvokeExpr) expr;
            List<SQLExpr> methodParameters = methodExpr.getParameters();

            String methodName = methodExpr.getMethodName();
            String fieldName = methodParameters.get(0).toString();
            Object spatialParamsObject = SpatialParamsFactory.generateSpatialParamsObject(methodName, methodParameters);

            Condition condition = new Condition(CONN.valueOf(opear), fieldName, methodName, spatialParamsObject);
            where.addWhere(condition);
        }else {
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
		} else if (expr instanceof SQLIdentifierExpr) {
            return expr;
        } else if (expr instanceof SQLPropertyExpr){
            return expr;
		} else {
			throw new SqlParseException(
					String.format("Failed to parse SqlExpression of type %s. expression value: %s", expr.getClass(), expr)
			);
		}
	}


    private void findSelect(MySqlSelectQueryBlock query, Select select,String tableAlias) throws SqlParseException {
        List<SQLSelectItem> selectList = query.getSelectList();
        for (SQLSelectItem sqlSelectItem : selectList) {
            Field field = FieldMaker.makeField(sqlSelectItem.getExpr(), sqlSelectItem.getAlias(),tableAlias);
            select.addField(field);
        }
    }

	private void findGroupBy(MySqlSelectQueryBlock query, Select select) throws SqlParseException {
		SQLSelectGroupByClause groupBy = query.getGroupBy();
		if (groupBy == null) {
			return;
		}
		List<SQLExpr> items = groupBy.getItems();

		List<SQLExpr> standardGroupBys = new ArrayList<>();
		for (SQLExpr sqlExpr : items) {
            //todo: mysql expr patch
            if (sqlExpr instanceof MySqlSelectGroupByExpr) {
                MySqlSelectGroupByExpr sqlSelectGroupByExpr = (MySqlSelectGroupByExpr) sqlExpr;
                sqlExpr = sqlSelectGroupByExpr.getExpr();
            }

             if (!(sqlExpr instanceof SQLIdentifierExpr) &&  !standardGroupBys.isEmpty()) {
				// flush the standard group bys
				select.addGroupBy(convertExprsToFields(standardGroupBys));
				standardGroupBys = new ArrayList<>();
			}

			if (sqlExpr instanceof SQLIdentifierExpr) {
				SQLIdentifierExpr identifierExpr = (SQLIdentifierExpr) sqlExpr;
					// single item without parens (should latch to before or after list)
					standardGroupBys.add(identifierExpr);

            } else if (sqlExpr instanceof SQLListExpr) {
				// multiple items in their own list
				SQLListExpr listExpr = (SQLListExpr) sqlExpr;
				select.addGroupBy(convertExprsToFields(listExpr.getItems()));
			} else {
				// something else
				standardGroupBys.add(sqlExpr);
			}
		}
		if (!standardGroupBys.isEmpty()) {
			select.addGroupBy(convertExprsToFields(standardGroupBys));
		}
	}

	private List<Field> convertExprsToFields(List<? extends SQLExpr> exprs) throws SqlParseException {
		List<Field> fields = new ArrayList<>(exprs.size());
		for (SQLExpr expr : exprs) {
			fields.add(FieldMaker.makeField(expr, null,null));
		}
		return fields;
	}

    private String sameAliasWhere(Where where, String... aliases) throws SqlParseException {
        if(where == null) return null;

        if(where instanceof Condition)
        {
            Condition condition = (Condition) where;
            String fieldName = condition.getName();
            for (String alias : aliases){
                String prefix = alias + ".";
                if(fieldName.startsWith(prefix)){
                    return alias;
                }
            }
            throw new SqlParseException(String.format("fieldName : %s on codition:%s does not contain alias", fieldName, condition.toString()));
        }
        List<String> sameAliases = new ArrayList<>();
        if(where.getWheres()!=null && where.getWheres().size() > 0) {
            for (Where innerWhere : where.getWheres())
                sameAliases.add(sameAliasWhere(innerWhere, aliases));
        }

        if ( sameAliases.contains(null) ) return null;
        if ( sameAliases.stream().distinct().count() != 1 ) return null;
        return sameAliases.get(0);
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
			lists.add(FieldMaker.makeField(expr, null,null).toString());
			if (sqlSelectOrderByItem.getType() == null) {
				sqlSelectOrderByItem.setType(SQLOrderingSpecification.ASC);
			}
			String type = sqlSelectOrderByItem.getType().toString();
			for (String name : lists) {
				name = name.replace("`", "");
				select.addOrderBy(name, type);
			}
			lists.clear();
		}

	}

	private void findLimit(MySqlSelectQueryBlock.Limit limit, Select select) {

		if (limit == null) {
			return;
		}

		select.setRowCount(Integer.parseInt(limit.getRowCount().toString()));

		if (limit.getOffset() != null)
			select.setOffset(Integer.parseInt(limit.getOffset().toString()));
	}

	/**
	 * Parse the from clause
	 * @param from the from clause.
	 * @return list of From objects represents all the sources.
	 */
	private List<From> findFrom(SQLTableSource from) {
        boolean isSqlExprTable = from.getClass().isAssignableFrom(SQLExprTableSource.class);

        if(isSqlExprTable){
            SQLExprTableSource fromExpr = (SQLExprTableSource) from;
            String[] split = fromExpr.getExpr().toString().split(",");

            ArrayList<From> fromList = new ArrayList<>();
            for (String source : split) {
                fromList.add(new From(source.trim(),fromExpr.getAlias()));
            }
            return fromList;
        }

        SQLJoinTableSource joinTableSource = ((SQLJoinTableSource) from);
        List<From> fromList = new ArrayList<>();
        fromList.addAll(findFrom(joinTableSource.getLeft()));
        fromList.addAll(findFrom(joinTableSource.getRight()));
        return fromList;
    }

    public JoinSelect parseJoinSelect(SQLQueryExpr sqlExpr) throws SqlParseException {

        MySqlSelectQueryBlock query = (MySqlSelectQueryBlock) sqlExpr.getSubQuery().getQuery();

        List<From> joinedFrom = findJoinedFrom(query.getFrom());
        if(joinedFrom.size() != 2)
            throw new RuntimeException("currently supports only 2 tables join");

        JoinSelect joinSelect = createBasicJoinSelectAccordingToTableSource((SQLJoinTableSource) query.getFrom());
        List<Hint> hints = parseHints(query.getHints());
        joinSelect.setHints(hints);
        String firstTableAlias = joinedFrom.get(0).getAlias();
        String secondTableAlias = joinedFrom.get(1).getAlias();
        Map<String, Where> aliasToWhere = splitAndFindWhere(query.getWhere(), firstTableAlias, secondTableAlias);

        fillTableSelectedJoin(joinSelect.getFirstTable(), query, joinedFrom.get(0), aliasToWhere.get(firstTableAlias), joinSelect.getConnectedConditions());
        fillTableSelectedJoin(joinSelect.getSecondTable(), query, joinedFrom.get(1), aliasToWhere.get(secondTableAlias), joinSelect.getConnectedConditions());

        updateJoinLimit(query.getLimit(), joinSelect);

        //todo: throw error feature not supported:  no group bys on joins ?
        return joinSelect;
    }

    private void updateJoinLimit(MySqlSelectQueryBlock.Limit limit, JoinSelect joinSelect) {
         if(limit != null  && limit.getRowCount()!= null) {
             int sizeLimit = Integer.parseInt(limit.getRowCount().toString());
             joinSelect.setTotalLimit(sizeLimit);
         }
    }

    private List<Hint> parseHints(List<SQLCommentHint> sqlHints) {
        List<Hint> hints = new ArrayList<>();
        for (SQLCommentHint sqlHint : sqlHints) {
            Hint hint = HintFactory.getHintFromString(sqlHint.getText());
            if (hint != null) hints.add(hint);
        }
        return hints;
    }

    private JoinSelect createBasicJoinSelectAccordingToTableSource(SQLJoinTableSource joinTableSource) throws SqlParseException {
        JoinSelect joinSelect = new JoinSelect();
        List<Condition> conditions = getJoinConditionsFlatten(joinTableSource);
        joinSelect.setConnectedConditions(conditions);
        SQLJoinTableSource.JoinType joinType = joinTableSource.getJoinType();
        joinSelect.setJoinType(joinType);
        return joinSelect;
    }

    private Map<String, Where> splitAndFindWhere(SQLExpr whereExpr, String firstTableAlias, String secondTableAlias) throws SqlParseException {
        Where where = findWhere(whereExpr);
        return splitWheres(where, firstTableAlias, secondTableAlias);
    }

    private void fillTableSelectedJoin(TableOnJoinSelect tableOnJoin,MySqlSelectQueryBlock query, From tableFrom,  Where where, List<Condition> conditions) throws SqlParseException {
        String alias = tableFrom.getAlias();
        fillBasicTableSelectJoin(tableOnJoin, tableFrom, where, query);
        tableOnJoin.setConnectedFields(getConnectedFields(conditions, alias));
        tableOnJoin.setSelectedFields(new ArrayList<Field>(tableOnJoin.getFields()));
        tableOnJoin.setAlias(alias);
    }

    private List<Field> getConnectedFields(List<Condition> conditions, String alias) throws SqlParseException {
        List<Field> fields = new ArrayList<>();
        String prefix = alias + ".";
        for(Condition condition : conditions) {
            if(condition.getName().startsWith(prefix)){
                fields.add(new Field(condition.getName().replaceFirst(prefix,""),null));
            }
            else {
                if(! (condition.getValue() instanceof SQLPropertyExpr)){
                    throw new SqlParseException("conditions on join should be one side is firstTable second Other , condition was:" + condition.toString());
                }
                SQLPropertyExpr conditionValue = (SQLPropertyExpr) condition.getValue();
                String aliasDotValue = conditionValue.toString();
                int indexOfDot = aliasDotValue.indexOf(".");
                String owner = aliasDotValue.substring(0, indexOfDot);
                if(owner.equals(alias))
                    fields.add(new Field(aliasDotValue.substring(indexOfDot+1),null));
            }
        }
        return fields;
    }

    private void fillBasicTableSelectJoin(TableOnJoinSelect select, From from,  Where where, MySqlSelectQueryBlock query) throws SqlParseException {
        select.getFrom().add(from);
        findSelect(query, select,from.getAlias());
        select.setWhere(where);
    }

    private List<Condition> getJoinConditionsFlatten(SQLJoinTableSource from) throws SqlParseException {
        List<Condition> conditions = new ArrayList<>();
        if(from.getCondition() == null ) return conditions;
        Where where = Where.newInstance();
        parseWhere(from.getCondition(), where);
        addIfConditionRecursive(where, conditions);
        return conditions;
    }


    private Map<String,Where> splitWheres(Where where, String... aliases) throws SqlParseException {
        Map<String,Where> aliasToWhere = new HashMap<>();
        for(String alias : aliases){
            aliasToWhere.put(alias,null);
        }
        if(where == null) return aliasToWhere;

        String allWhereFromSameAlias = sameAliasWhere(where, aliases);
        if( allWhereFromSameAlias != null ) {
            removeAliasPrefix(where,allWhereFromSameAlias);
            aliasToWhere.put(allWhereFromSameAlias,where);
            return aliasToWhere;
        }
        for(Where innerWhere : where.getWheres()){
            String sameAlias =  sameAliasWhere(innerWhere, aliases);
            if(sameAlias == null )
                throw new SqlParseException("Currently support only one hierarchy on different tables where");
            removeAliasPrefix(innerWhere,sameAlias);
            Where aliasCurrentWhere = aliasToWhere.get(sameAlias);
            if(aliasCurrentWhere == null) {
                aliasToWhere.put(sameAlias, innerWhere);
            }
            else {
                Where andWhereContainer = Where.newInstance();
                andWhereContainer.addWhere(aliasCurrentWhere);
                andWhereContainer.addWhere(innerWhere);
                aliasToWhere.put(sameAlias,andWhereContainer);
            }
        }

        return  aliasToWhere;
    }

    private void removeAliasPrefix(Where where, String alias) {

        if(where instanceof Condition) {
            Condition cond = (Condition) where;
            String fieldName = cond.getName();
            String aliasPrefix = alias + ".";
            cond.setName(cond.getName().replaceFirst(aliasPrefix, ""));
            return;
        }
        for(Where innerWhere : where.getWheres())
        {
            removeAliasPrefix(innerWhere, alias);
        }
    }

    private void addIfConditionRecursive(Where where, List<Condition> conditions) throws SqlParseException {
        if(where instanceof Condition){
            Condition cond = (Condition) where;
            if( ! (cond.getValue() instanceof  SQLPropertyExpr)){
                throw new SqlParseException("conditions on join should be one side is firstTable second Other , condition was:" + cond.toString());
            }
            conditions.add(cond);
        }
        for(Where innerWhere : where.getWheres())
        {
            addIfConditionRecursive(innerWhere,conditions);
        }
    }

    private List<From> findJoinedFrom(SQLTableSource from) {
        SQLJoinTableSource joinTableSource = ((SQLJoinTableSource) from);
        List<From> fromList = new ArrayList<>();
        fromList.addAll(findFrom(joinTableSource.getLeft()));
        fromList.addAll(findFrom(joinTableSource.getRight()));
        return fromList;
    }

}
