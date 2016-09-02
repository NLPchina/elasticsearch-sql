package org.nlpcn.es4sql.parse;

import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.nlpcn.es4sql.SQLFunctions;
import org.nlpcn.es4sql.Util;
import org.nlpcn.es4sql.domain.*;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.spatial.SpatialParamsFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by allwefantasy on 9/2/16.
 */
public class WhereParser {

    private MySqlSelectQueryBlock query;
    private SQLDeleteStatement delete;
    private SQLExpr where;
    private SqlParser sqlParser;

    public WhereParser(SqlParser sqlParser, MySqlSelectQueryBlock query) {
        this.sqlParser = sqlParser;
        this.query = query;
        this.where = query.getWhere();
    }

    public WhereParser(SqlParser sqlParser, SQLDeleteStatement delete) {
        this.sqlParser = sqlParser;
        this.delete = delete;
        this.where = delete.getWhere();
    }

    public WhereParser(SqlParser sqlParser, SQLExpr expr) {
        this.sqlParser = sqlParser;
        this.where = expr;
    }

    public WhereParser(SqlParser sqlParser) {
        this.sqlParser = sqlParser;
    }

    public Where findWhere() throws SqlParseException {
        if (where == null) {
            return null;
        }

        Where myWhere = Where.newInstance();
        parseWhere(where, myWhere);
        return myWhere;
    }

    public void parseWhere(SQLExpr expr, Where where) throws SqlParseException {


        if (expr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr bExpr = (SQLBinaryOpExpr) expr;
            if (explanSpecialCondWithBothSidesAreLiterals(bExpr, where)) {
                return;
            }
            if (explanSpecialCondWithBothSidesAreProperty(bExpr, where)) {
                return;
            }
        }

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

    //some where conditions eg. 1=1 or 3>2 or 'a'='b'
    private boolean explanSpecialCondWithBothSidesAreLiterals(SQLBinaryOpExpr bExpr, Where where) throws SqlParseException {
        if ((bExpr.getLeft() instanceof SQLNumericLiteralExpr || bExpr.getLeft() instanceof SQLCharExpr) &&
                (bExpr.getRight() instanceof SQLNumericLiteralExpr || bExpr.getRight() instanceof SQLCharExpr)
                ) {
            SQLMethodInvokeExpr sqlMethodInvokeExpr = new SQLMethodInvokeExpr("script", null);
            String operator = bExpr.getOperator().getName();
            if (operator.equals("=")) {
                operator = "==";
            }
            sqlMethodInvokeExpr.addParameter(
                    new SQLCharExpr(Util.expr2Object(bExpr.getLeft(), "'") +
                            " " + operator + " " +
                            Util.expr2Object(bExpr.getRight(), "'"))
            );

            explanCond("AND", sqlMethodInvokeExpr, where);
            return true;
        }
        return false;
    }

    //some where conditions eg. field1=field2 or field1>field2
    private boolean explanSpecialCondWithBothSidesAreProperty(SQLBinaryOpExpr bExpr, Where where) throws SqlParseException {
        //join is not support
        if ((bExpr.getLeft() instanceof SQLPropertyExpr || bExpr.getLeft() instanceof SQLIdentifierExpr) &&
                (bExpr.getRight() instanceof SQLPropertyExpr || bExpr.getRight() instanceof SQLIdentifierExpr) &&
                Sets.newHashSet("=", "<", ">", ">=", "<=").contains(bExpr.getOperator().getName()) &&
                !Util.isFromJoinTable(bExpr)

                ) {
            SQLMethodInvokeExpr sqlMethodInvokeExpr = new SQLMethodInvokeExpr("script", null);
            String operator = bExpr.getOperator().getName();
            if (operator.equals("=")) {
                operator = "==";
            }

            String leftProperty = Util.expr2Object(bExpr.getLeft()).toString();
            String rightProperty = Util.expr2Object(bExpr.getRight()).toString();
            if (leftProperty.split("\\.").length > 1) {

                leftProperty = leftProperty.substring(leftProperty.split("\\.")[0].length() + 1);
            }

            if (rightProperty.split("\\.").length > 1) {
                rightProperty = rightProperty.substring(rightProperty.split("\\.")[0].length() + 1);
            }

            sqlMethodInvokeExpr.addParameter(new SQLCharExpr(
                    "doc['" + leftProperty + "'].value " +
                            operator +
                            " doc['" + rightProperty + "'].value"));


            explanCond("AND", sqlMethodInvokeExpr, where);
            return true;
        }
        return false;
    }


    private boolean isCond(SQLBinaryOpExpr expr) {
        SQLExpr leftSide = expr.getLeft();
        if (leftSide instanceof SQLMethodInvokeExpr) {
            return isAllowedMethodOnConditionLeft((SQLMethodInvokeExpr) leftSide, expr.getOperator());
        }
        return leftSide instanceof SQLIdentifierExpr ||
                leftSide instanceof SQLPropertyExpr ||
                leftSide instanceof SQLVariantRefExpr;
    }

    private boolean isAllowedMethodOnConditionLeft(SQLMethodInvokeExpr method, SQLBinaryOperator operator) {
        return (method.getMethodName().toLowerCase().equals("nested") ||
                method.getMethodName().toLowerCase().equals("children") ||
                SQLFunctions.buildInFunctions.contains(method.getMethodName().toLowerCase())
        ) &&
                !operator.isLogical();
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

            boolean isNested = false;
            boolean isChildren = false;

            NestedType nestedType = new NestedType();
            if (nestedType.tryFillFromExpr(soExpr.getLeft())) {
                soExpr.setLeft(new SQLIdentifierExpr(nestedType.field));
                isNested = true;
            }

            ChildrenType childrenType = new ChildrenType();
            if (childrenType.tryFillFromExpr(soExpr.getLeft())) {
                soExpr.setLeft(new SQLIdentifierExpr(childrenType.field));
                isChildren = true;
            }

            if (soExpr.getRight() instanceof SQLMethodInvokeExpr) {
                SQLMethodInvokeExpr method = (SQLMethodInvokeExpr) soExpr.getRight();
                String methodName = method.getMethodName().toLowerCase();

                if (Condition.OPEAR.methodNameToOpear.containsKey(methodName)) {
                    Object[] methodParametersValue = getMethodValuesWithSubQueries(method);

                    Condition condition = null;

                    if (isNested)
                        condition = new Condition(Where.CONN.valueOf(opear), soExpr.getLeft().toString(), Condition.OPEAR.methodNameToOpear.get(methodName), methodParametersValue, nestedType);
                    else if (isChildren)
                        condition = new Condition(Where.CONN.valueOf(opear), soExpr.getLeft().toString(), Condition.OPEAR.methodNameToOpear.get(methodName), methodParametersValue, childrenType);
                    else
                        condition = new Condition(Where.CONN.valueOf(opear), soExpr.getLeft().toString(), Condition.OPEAR.methodNameToOpear.get(methodName), methodParametersValue, null);

                    where.addWhere(condition);
                    methodAsOpear = true;
                }
            }

            if (!methodAsOpear) {
                Condition condition = null;

                if (isNested)
                    condition = new Condition(Where.CONN.valueOf(opear), soExpr.getLeft().toString(), soExpr.getOperator().name, parseValue(soExpr.getRight()), nestedType);
                else if (isChildren)
                    condition = new Condition(Where.CONN.valueOf(opear), soExpr.getLeft().toString(), soExpr.getOperator().name, parseValue(soExpr.getRight()), childrenType);
                else {
                    SQLMethodInvokeExpr sqlMethodInvokeExpr = parseSQLBinaryOpExprWhoIsConditionInWhere(soExpr);
                    if (sqlMethodInvokeExpr == null) {
                        condition = new Condition(Where.CONN.valueOf(opear), soExpr.getLeft().toString(), soExpr.getOperator().name, parseValue(soExpr.getRight()), null);
                    } else {
                        ScriptFilter scriptFilter = new ScriptFilter();
                        if (!scriptFilter.tryParseFromMethodExpr(sqlMethodInvokeExpr)) {
                            throw new SqlParseException("could not parse script filter");
                        }
                        condition = new Condition(Where.CONN.valueOf(opear), null, "SCRIPT", scriptFilter);

                    }

                }
                where.addWhere(condition);
            }
        } else if (expr instanceof SQLInListExpr) {
            SQLInListExpr siExpr = (SQLInListExpr) expr;
            String leftSide = siExpr.getExpr().toString();

            boolean isNested = false;
            boolean isChildren = false;

            NestedType nestedType = new NestedType();
            if (nestedType.tryFillFromExpr(siExpr.getExpr())) {
                leftSide = nestedType.field;

                isNested = false;
            }

            ChildrenType childrenType = new ChildrenType();
            if (childrenType.tryFillFromExpr(siExpr.getExpr())) {
                leftSide = childrenType.field;

                isChildren = true;
            }

            Condition condition = null;

            if (isNested)
                condition = new Condition(Where.CONN.valueOf(opear), leftSide, siExpr.isNot() ? "NOT IN" : "IN", parseValue(siExpr.getTargetList()), nestedType);
            else if (isChildren)
                condition = new Condition(Where.CONN.valueOf(opear), leftSide, siExpr.isNot() ? "NOT IN" : "IN", parseValue(siExpr.getTargetList()), childrenType);
            else
                condition = new Condition(Where.CONN.valueOf(opear), leftSide, siExpr.isNot() ? "NOT IN" : "IN", parseValue(siExpr.getTargetList()), null);

            where.addWhere(condition);
        } else if (expr instanceof SQLBetweenExpr) {
            SQLBetweenExpr between = ((SQLBetweenExpr) expr);
            String leftSide = between.getTestExpr().toString();

            boolean isNested = false;
            boolean isChildren = false;

            NestedType nestedType = new NestedType();
            if (nestedType.tryFillFromExpr(between.getTestExpr())) {
                leftSide = nestedType.field;

                isNested = true;
            }

            ChildrenType childrenType = new ChildrenType();
            if (childrenType.tryFillFromExpr(between.getTestExpr())) {
                leftSide = childrenType.field;

                isChildren = true;
            }

            Condition condition = null;

            if (isNested)
                condition = new Condition(Where.CONN.valueOf(opear), leftSide, between.isNot() ? "NOT BETWEEN" : "BETWEEN", new Object[]{parseValue(between.beginExpr), parseValue(between.endExpr)}, nestedType);
            else if (isChildren)
                condition = new Condition(Where.CONN.valueOf(opear), leftSide, between.isNot() ? "NOT BETWEEN" : "BETWEEN", new Object[]{parseValue(between.beginExpr), parseValue(between.endExpr)}, childrenType);
            else
                condition = new Condition(Where.CONN.valueOf(opear), leftSide, between.isNot() ? "NOT BETWEEN" : "BETWEEN", new Object[]{parseValue(between.beginExpr), parseValue(between.endExpr)}, null);

            where.addWhere(condition);
        } else if (expr instanceof SQLMethodInvokeExpr) {

            SQLMethodInvokeExpr methodExpr = (SQLMethodInvokeExpr) expr;
            List<SQLExpr> methodParameters = methodExpr.getParameters();

            String methodName = methodExpr.getMethodName();
            if (SpatialParamsFactory.isAllowedMethod(methodName)) {
                String fieldName = methodParameters.get(0).toString();

                boolean isNested = false;
                boolean isChildren = false;

                NestedType nestedType = new NestedType();
                if (nestedType.tryFillFromExpr(methodParameters.get(0))) {
                    fieldName = nestedType.field;

                    isNested = true;
                }

                ChildrenType childrenType = new ChildrenType();
                if (childrenType.tryFillFromExpr(methodParameters.get(0))) {
                    fieldName = childrenType.field;

                    isChildren = true;
                }

                Object spatialParamsObject = SpatialParamsFactory.generateSpatialParamsObject(methodName, methodParameters);

                Condition condition = null;

                if (isNested)
                    condition = new Condition(Where.CONN.valueOf(opear), fieldName, methodName, spatialParamsObject, nestedType);
                else if (isChildren)
                    condition = new Condition(Where.CONN.valueOf(opear), fieldName, methodName, spatialParamsObject, childrenType);
                else
                    condition = new Condition(Where.CONN.valueOf(opear), fieldName, methodName, spatialParamsObject, null);

                where.addWhere(condition);
            } else if (methodName.toLowerCase().equals("nested")) {
                NestedType nestedType = new NestedType();

                if (!nestedType.tryFillFromExpr(expr)) {
                    throw new SqlParseException("could not fill nested from expr:" + expr);
                }

                Condition condition = new Condition(Where.CONN.valueOf(opear), nestedType.path, methodName.toUpperCase(), nestedType.where);

                where.addWhere(condition);
            } else if (methodName.toLowerCase().equals("children")) {
                ChildrenType childrenType = new ChildrenType();

                if (!childrenType.tryFillFromExpr(expr)) {
                    throw new SqlParseException("could not fill children from expr:" + expr);
                }

                Condition condition = new Condition(Where.CONN.valueOf(opear), childrenType.childType, methodName.toUpperCase(), childrenType.where);

                where.addWhere(condition);
            } else if (methodName.toLowerCase().equals("script")) {
                ScriptFilter scriptFilter = new ScriptFilter();
                if (!scriptFilter.tryParseFromMethodExpr(methodExpr)) {
                    throw new SqlParseException("could not parse script filter");
                }
                Condition condition = new Condition(Where.CONN.valueOf(opear), null, "SCRIPT", scriptFilter);
                where.addWhere(condition);
            } else {
                throw new SqlParseException("unsupported method: " + methodName);
            }
        } else if (expr instanceof SQLInSubQueryExpr) {
            SQLInSubQueryExpr sqlIn = (SQLInSubQueryExpr) expr;

            Select innerSelect = sqlParser.parseSelect((MySqlSelectQueryBlock) sqlIn.getSubQuery().getQuery());

            if (innerSelect.getFields() == null || innerSelect.getFields().size() != 1)
                throw new SqlParseException("should only have one return field in subQuery");

            SubQueryExpression subQueryExpression = new SubQueryExpression(innerSelect);

            String leftSide = sqlIn.getExpr().toString();

            boolean isNested = false;
            boolean isChildren = false;

            NestedType nestedType = new NestedType();
            if (nestedType.tryFillFromExpr(sqlIn.getExpr())) {
                leftSide = nestedType.field;

                isNested = true;
            }

            ChildrenType childrenType = new ChildrenType();
            if (childrenType.tryFillFromExpr(sqlIn.getExpr())) {
                leftSide = childrenType.field;

                isChildren = true;
            }

            Condition condition = null;

            if (isNested)
                condition = new Condition(Where.CONN.valueOf(opear), leftSide, sqlIn.isNot() ? "NOT IN" : "IN", subQueryExpression, nestedType);
            else if (isChildren)
                condition = new Condition(Where.CONN.valueOf(opear), leftSide, sqlIn.isNot() ? "NOT IN" : "IN", subQueryExpression, childrenType);
            else
                condition = new Condition(Where.CONN.valueOf(opear), leftSide, sqlIn.isNot() ? "NOT IN" : "IN", subQueryExpression, null);

            where.addWhere(condition);
        } else {
            throw new SqlParseException("err find condition " + expr.getClass());
        }
    }

    private MethodField parseSQLMethodInvokeExprWithFunctionInWhere(SQLMethodInvokeExpr soExpr) throws SqlParseException {

        MethodField methodField = FieldMaker.makeMethodField(soExpr.getMethodName(),
                soExpr.getParameters(),
                null,
                null,
                query == null ? query.getFrom().getAlias() : null,
                false);
        return methodField;
    }

    private SQLMethodInvokeExpr parseSQLBinaryOpExprWhoIsConditionInWhere(SQLBinaryOpExpr soExpr) throws SqlParseException {

        if (!(soExpr.getLeft() instanceof SQLMethodInvokeExpr ||
                soExpr.getRight() instanceof SQLMethodInvokeExpr)) {
            return null;
        }

        if (soExpr.getLeft() instanceof SQLMethodInvokeExpr) {
            if (!SQLFunctions.buildInFunctions.contains(((SQLMethodInvokeExpr) soExpr.getLeft()).getMethodName())) {
                return null;
            }
        }

        if (soExpr.getRight() instanceof SQLMethodInvokeExpr) {
            if (!SQLFunctions.buildInFunctions.contains(((SQLMethodInvokeExpr) soExpr.getRight()).getMethodName())) {
                return null;
            }
        }


        MethodField leftMethod = new MethodField(null, Lists.newArrayList(new KVValue("", Util.expr2Object(soExpr.getLeft(), "'"))), null, null);
        MethodField rightMethod = new MethodField(null, Lists.newArrayList(new KVValue("", Util.expr2Object(soExpr.getRight(), "'"))), null, null);

        if (soExpr.getLeft() instanceof SQLMethodInvokeExpr) {
            leftMethod = parseSQLMethodInvokeExprWithFunctionInWhere((SQLMethodInvokeExpr) soExpr.getLeft());
        }
        if (soExpr.getRight() instanceof SQLMethodInvokeExpr) {
            rightMethod = parseSQLMethodInvokeExprWithFunctionInWhere((SQLMethodInvokeExpr) soExpr.getRight());
        }

        String v1 = leftMethod.getParams().get(0).value.toString();
        String v1Dec = leftMethod.getParams().size() == 2 ? leftMethod.getParams().get(1).value.toString() + ";" : "";


        String v2 = rightMethod.getParams().get(0).value.toString();
        String v2Dec = rightMethod.getParams().size() == 2 ? rightMethod.getParams().get(1).value.toString() + ";" : "";

        String operator = soExpr.getOperator().getName();

        if (operator.equals("=")) {
            operator = "==";
        }

        String finalStr = v1Dec + v2Dec + v1 + " " + operator + " " + v2;

        SQLMethodInvokeExpr scriptMethod = new SQLMethodInvokeExpr("script", null);
        scriptMethod.addParameter(new SQLCharExpr(finalStr));
        return scriptMethod;

    }

    private Object[] getMethodValuesWithSubQueries(SQLMethodInvokeExpr method) throws SqlParseException {
        List<Object> values = new ArrayList<>();
        for (SQLExpr innerExpr : method.getParameters()) {
            if (innerExpr instanceof SQLQueryExpr) {
                Select select = sqlParser.parseSelect((MySqlSelectQueryBlock) ((SQLQueryExpr) innerExpr).getSubQuery().getQuery());
                values.add(new SubQueryExpression(select));
            } else if (innerExpr instanceof SQLTextLiteralExpr) {
                values.add(((SQLTextLiteralExpr) innerExpr).getText());
            } else {
                values.add(innerExpr);
            }

        }
        return values.toArray();
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
        } else if (expr instanceof SQLPropertyExpr) {
            return expr;
        } else {
            throw new SqlParseException(
                    String.format("Failed to parse SqlExpression of type %s. expression value: %s", expr.getClass(), expr)
            );
        }
    }
}
