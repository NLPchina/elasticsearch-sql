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
import org.nlpcn.es4sql.query.join.NestedLoopsElasticRequestBuilder;
import org.nlpcn.es4sql.spatial.SpatialParamsFactory;

/**
 * es sql support
 * 
 * @author ansj
 * 
 */
public class SqlParser {

	public SqlParser() {
	}

	public Select parseSelect(SQLQueryExpr mySqlExpr) throws SqlParseException {

		MySqlSelectQueryBlock query = (MySqlSelectQueryBlock) mySqlExpr.getSubQuery().getQuery();

        Select select = parseSelect(query);

		return select;
	}

    private Select parseSelect(MySqlSelectQueryBlock query) throws SqlParseException {
        Select select = new Select();

        findSelect(query, select,null);

        select.getFrom().addAll(findFrom(query.getFrom()));

        select.setWhere(findWhere(query.getWhere()));

        select.fillSubQueries();

        select.getHints().addAll(parseHints(query.getHints()));

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
        SQLExpr leftSide = expr.getLeft();
        if(leftSide instanceof SQLMethodInvokeExpr){
            return isAllowedMethodOnConditionLeft((SQLMethodInvokeExpr) leftSide);
        }
		return leftSide instanceof SQLIdentifierExpr || leftSide instanceof SQLPropertyExpr || leftSide instanceof SQLVariantRefExpr;
	}

    private boolean isAllowedMethodOnConditionLeft(SQLMethodInvokeExpr method) {
        return  method.getMethodName().toLowerCase().equals("nested");
    }

    public void parseWhere(SQLExpr expr, Where where) throws SqlParseException {
		if (expr instanceof SQLBinaryOpExpr && !isCond((SQLBinaryOpExpr) expr)) {
			SQLBinaryOpExpr bExpr = (SQLBinaryOpExpr) expr;
			routeCond(bExpr, bExpr.getLeft(), where);
			routeCond(bExpr, bExpr.getRight(), where);
		} else if (expr instanceof SQLNotExpr) {
			parseWhere(((SQLNotExpr) expr).getExpr(), where);
			negateWhere(where);
		} else {
			explanCond("AND", expr, where);
		}
	}

	private void routeCond(SQLBinaryOpExpr bExpr, SQLExpr sub, Where where) throws SqlParseException {
		if (sub instanceof SQLBinaryOpExpr && !isCond((SQLBinaryOpExpr) sub)) {
			SQLBinaryOpExpr binarySub = (SQLBinaryOpExpr) sub;
			if (binarySub.getOperator().priority != bExpr.getOperator().priority) {
				Where subWhere = new Where(bExpr.getOperator().name);
				where.addWhere(subWhere);
				parseWhere(binarySub, subWhere);
			} else {
				parseWhere(binarySub, where);
			}
		} else if (sub instanceof SQLNotExpr) {
			Where subWhere = new Where(bExpr.getOperator().name);
			where.addWhere(subWhere);
			parseWhere(((SQLNotExpr) sub).getExpr(), subWhere);
			negateWhere(subWhere);
		} else {
			explanCond(bExpr.getOperator().name, sub, where);
		}
	}

	private void explanCond(String opear, SQLExpr expr, Where where) throws SqlParseException {
        if (expr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr soExpr = (SQLBinaryOpExpr) expr;
            boolean methodAsOpear = false;
            boolean nestedFieldCondition = false;
            String nestedPath = null ;
            NestedType nestedType = new NestedType();
            if(nestedType.tryFillFromExpr(soExpr.getLeft())){
                soExpr.setLeft(new SQLIdentifierExpr(nestedType.field));
                nestedFieldCondition = true;
                nestedPath = nestedType.path ;
            }

            if(soExpr.getRight() instanceof SQLMethodInvokeExpr){
                SQLMethodInvokeExpr method = (SQLMethodInvokeExpr) soExpr.getRight();
                String methodName = method.getMethodName().toLowerCase();

                if(Condition.OPEAR.methodNameToOpear.containsKey(methodName)){
                    Object[] methodParametersValue = getMethodValuesWithSubQueries(method);
                    Condition condition = new Condition(CONN.valueOf(opear) ,soExpr.getLeft().toString(), Condition.OPEAR.methodNameToOpear.get(methodName),methodParametersValue,nestedFieldCondition,nestedPath);
                    where.addWhere(condition);
                    methodAsOpear = true;
                }
            }
            if(!methodAsOpear){
                Condition condition = new Condition(CONN.valueOf(opear), soExpr.getLeft().toString(), soExpr.getOperator().name, parseValue(soExpr.getRight()),nestedFieldCondition,nestedPath);
                where.addWhere(condition);
            }
        } else if (expr instanceof SQLInListExpr) {
            SQLInListExpr siExpr = (SQLInListExpr) expr;
            NestedType nestedType = new NestedType();
            String leftSide = siExpr.getExpr().toString();
            if(nestedType.tryFillFromExpr(siExpr.getExpr())){
                leftSide = nestedType.field;
            }
            Condition condition = new Condition(CONN.valueOf(opear), leftSide, siExpr.isNot() ? "NOT IN" : "IN", parseValue(siExpr.getTargetList()),nestedType.field!=null,nestedType.path);
            where.addWhere(condition);
        } else if (expr instanceof SQLBetweenExpr) {
            SQLBetweenExpr between = ((SQLBetweenExpr) expr);
            String leftSide = between.getTestExpr().toString();
            NestedType nestedType = new NestedType();
            if(nestedType.tryFillFromExpr(between.getTestExpr())){
                leftSide = nestedType.field;
            }
            Condition condition = new Condition(CONN.valueOf(opear), leftSide, between.isNot() ? "NOT BETWEEN" : "BETWEEN", new Object[]{parseValue(between.beginExpr),
                    parseValue(between.endExpr)},nestedType.field!=null,nestedType.path);
            where.addWhere(condition);
        }
        else if (expr instanceof SQLMethodInvokeExpr) {
            SQLMethodInvokeExpr methodExpr = (SQLMethodInvokeExpr) expr;
            List<SQLExpr> methodParameters = methodExpr.getParameters();

            String methodName = methodExpr.getMethodName();
            String fieldName = methodParameters.get(0).toString();
            NestedType nestedType = new NestedType();
            if(nestedType.tryFillFromExpr(methodParameters.get(0))){
                fieldName = nestedType.field;
            }

            Object spatialParamsObject = SpatialParamsFactory.generateSpatialParamsObject(methodName, methodParameters);

            Condition condition = new Condition(CONN.valueOf(opear), fieldName, methodName, spatialParamsObject,nestedType.field!=null,nestedType.path);
            where.addWhere(condition);
        } else if (expr instanceof SQLInSubQueryExpr){
            SQLInSubQueryExpr sqlIn = (SQLInSubQueryExpr) expr;
            Select innerSelect = parseSelect((MySqlSelectQueryBlock) sqlIn.getSubQuery().getQuery());
            if(innerSelect.getFields() == null || innerSelect.getFields().size()!=1)
                throw new SqlParseException("should only have one return field in subQuery");
            SubQueryExpression subQueryExpression = new SubQueryExpression(innerSelect);
            String leftSide = sqlIn.getExpr().toString();
            NestedType nestedType = new NestedType();
            if(nestedType.tryFillFromExpr(sqlIn.getExpr())){
                leftSide = nestedType.field;
            }
            Condition condition = new Condition(CONN.valueOf(opear), leftSide, sqlIn.isNot() ? "NOT IN" : "IN",subQueryExpression,nestedType.field!=null,nestedType.path);
            where.addWhere(condition);
        } else {
			throw new SqlParseException("err find condition " + expr.getClass());
		}
	}



    private Object[] getMethodValuesWithSubQueries(SQLMethodInvokeExpr method) throws SqlParseException {
        List<Object> values = new ArrayList<>();
        boolean foundSubQuery = false;
        for(SQLExpr innerExpr : method.getParameters()){
            if(innerExpr instanceof SQLQueryExpr){
                foundSubQuery = true;
                Select select = parseSelect((MySqlSelectQueryBlock) ((SQLQueryExpr) innerExpr).getSubQuery().getQuery());
                values.add(new SubQueryExpression(select));
            }
            else {
                values.add(innerExpr);
            }

        }
        Object[] conditionValues ;
        if(foundSubQuery)
            conditionValues = values.toArray();
        else
            conditionValues = method.getParameters().toArray();
        return conditionValues;
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

            if ((sqlExpr instanceof SQLParensIdentifierExpr || !(sqlExpr instanceof SQLIdentifierExpr|| sqlExpr instanceof SQLMethodInvokeExpr)) && !standardGroupBys.isEmpty()) {
                // flush the standard group bys
                select.addGroupBy(convertExprsToFields(standardGroupBys));
                standardGroupBys = new ArrayList<>();
            }

			if (sqlExpr instanceof SQLParensIdentifierExpr) {
                // single item with parens (should get its own aggregation)
                select.addGroupBy(FieldMaker.makeField(sqlExpr, null,null));
            } else if (sqlExpr instanceof SQLListExpr) {
				// multiple items in their own list
				SQLListExpr listExpr = (SQLListExpr) sqlExpr;
				select.addGroupBy(convertExprsToFields(listExpr.getItems()));
			} else {
				// everything else gets added to the running list of standard group bys
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
        String firstAlias = sameAliases.get(0);
        //return null if more than one alias
        for(String alias : sameAliases){
            if(!alias.equals(firstAlias)) return null;
        }
        return firstAlias;
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
        List<Condition> connectedConditions = getConditionsFlatten(joinSelect.getConnectedWhere());
        joinSelect.setConnectedConditions(connectedConditions);
        fillTableSelectedJoin(joinSelect.getFirstTable(), query, joinedFrom.get(0), aliasToWhere.get(firstTableAlias), connectedConditions);
        fillTableSelectedJoin(joinSelect.getSecondTable(), query, joinedFrom.get(1), aliasToWhere.get(secondTableAlias), connectedConditions);

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
        if(joinTableSource.getCondition() != null ) {
            Where where = Where.newInstance();
            parseWhere(joinTableSource.getCondition(), where);
            joinSelect.setConnectedWhere(where);
        }
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
        tableOnJoin.fillSubQueries();
    }

    private List<Field> getConnectedFields(List<Condition> conditions, String alias) throws SqlParseException {
        List<Field> fields = new ArrayList<>();
        String prefix = alias + ".";
        for(Condition condition : conditions) {
            if(condition.getName().startsWith(prefix)){
                fields.add(new Field(condition.getName().replaceFirst(prefix,""),null));
            }
            else {
                if(! ((condition.getValue() instanceof SQLPropertyExpr)||(condition.getValue() instanceof SQLIdentifierExpr)||(condition.getValue() instanceof String))){
                    throw new SqlParseException("conditions on join should be one side is firstTable second Other , condition was:" + condition.toString());
                }
                String aliasDotValue = condition.getValue().toString();
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

    private List<Condition> getConditionsFlatten(Where where) throws SqlParseException {
        List<Condition> conditions = new ArrayList<>();
        if(where == null) return conditions;
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
            if( ! ((cond.getValue() instanceof  SQLIdentifierExpr) ||(cond.getValue() instanceof  SQLPropertyExpr)|| (cond.getValue() instanceof  String))){
                throw new SqlParseException("conditions on join should be one side is secondTable OPEAR firstTable, condition was:" + cond.toString());
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

	private void negateWhere(Where where) throws SqlParseException {
		for (Where sub : where.getWheres()) {
			if (sub instanceof Condition) {
				Condition cond = (Condition) sub;
				cond.setOpear(cond.getOpear().negative());
			} else {
				negateWhere(sub);
			}
            sub.setConn(sub.getConn().negative());
		}
	}

}
