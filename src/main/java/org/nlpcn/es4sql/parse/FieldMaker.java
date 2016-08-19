package org.nlpcn.es4sql.parse;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.parser.SQLParseException;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.elasticsearch.common.collect.Tuple;
import org.nlpcn.es4sql.Util;
import org.nlpcn.es4sql.domain.Field;
import org.nlpcn.es4sql.domain.KVValue;
import org.nlpcn.es4sql.domain.MethodField;
import org.nlpcn.es4sql.domain.Where;
import org.nlpcn.es4sql.exception.SqlParseException;
import com.alibaba.druid.sql.ast.*;
import org.nlpcn.es4sql.query.maker.AggMaker;
import org.nlpcn.es4sql.sql.SQLFunctions;

/**
 * 一些具有参数的一般在 select 函数.或者group by 函数
 *
 * @author ansj
 */
public class FieldMaker {
    public static Field makeField(SQLExpr expr, String alias, String tableAlias) throws SqlParseException {
        if (expr instanceof SQLIdentifierExpr || expr instanceof SQLPropertyExpr || expr instanceof SQLVariantRefExpr) {
            return handleIdentifier(expr, alias, tableAlias);
        } else if (expr instanceof SQLQueryExpr) {
            throw new SqlParseException("unknow field name : " + expr);
        } else if (expr instanceof SQLBinaryOpExpr) {
            //make a SCRIPT method field;
            return makeScriptMethodField((SQLBinaryOpExpr) expr, alias);

        } else if (expr instanceof SQLAllColumnExpr) {
        } else if (expr instanceof SQLMethodInvokeExpr) {
            SQLMethodInvokeExpr mExpr = (SQLMethodInvokeExpr) expr;
            String methodName = mExpr.getMethodName();
            if (methodName.toLowerCase().equals("nested") || methodName.toLowerCase().equals("reverse_nested")) {
                NestedType nestedType = new NestedType();
                if (nestedType.tryFillFromExpr(mExpr)) {
                    return handleIdentifier(nestedType, alias, tableAlias);
                }
            } else if (methodName.toLowerCase().equals("filter")) {
                return makeFilterMethodField(mExpr, alias);
            }
            return makeMethodField(methodName, mExpr.getParameters(), null, alias, true);
        } else if (expr instanceof SQLAggregateExpr) {
            SQLAggregateExpr sExpr = (SQLAggregateExpr) expr;
            return makeMethodField(sExpr.getMethodName(), sExpr.getArguments(), sExpr.getOption(), alias, true);
        } else {
            throw new SqlParseException("unknown field name : " + expr);
        }
        return null;
    }


    private static Field makeFilterMethodField(SQLMethodInvokeExpr filterMethod, String alias) throws SqlParseException {
        List<SQLExpr> parameters = filterMethod.getParameters();
        int parametersSize = parameters.size();
        if (parametersSize != 1 && parametersSize != 2) {
            throw new SqlParseException("filter group by field should only have one or 2 parameters filter(Expr) or filter(name,Expr)");
        }
        String filterAlias = filterMethod.getMethodName();
        SQLExpr exprToCheck = null;
        if (parametersSize == 1) {
            exprToCheck = parameters.get(0);
            filterAlias = "filter(" + exprToCheck.toString().replaceAll("\n", " ") + ")";
        }
        if (parametersSize == 2) {
            filterAlias = Util.extendedToString(parameters.get(0));
            exprToCheck = parameters.get(1);
        }
        Where where = Where.newInstance();
        new SqlParser().parseWhere(exprToCheck, where);
        if (where.getWheres().size() == 0)
            throw new SqlParseException("unable to parse filter where.");
        List<KVValue> methodParameters = new ArrayList<>();
        methodParameters.add(new KVValue("where", where));
        methodParameters.add(new KVValue("alias", filterAlias + "@FILTER"));
        return new MethodField("filter", methodParameters, null, alias);
    }


    private static Field handleIdentifier(NestedType nestedType, String alias, String tableAlias) throws SqlParseException {
        Field field = handleIdentifier(new SQLIdentifierExpr(nestedType.field), alias, tableAlias);
        field.setNested(nestedType);
        return field;
    }

    private static Field makeScriptMethodField(SQLBinaryOpExpr binaryExpr, String alias) throws SqlParseException {
        List<SQLExpr> params = new ArrayList<>();

        String scriptFieldAlias;
        if (alias == null || alias.equals(""))
            scriptFieldAlias = binaryExpr.toString();
        else
            scriptFieldAlias = alias;
        params.add(new SQLCharExpr(scriptFieldAlias));

        Object left = getScriptValue(binaryExpr.getLeft());
        Object right = getScriptValue(binaryExpr.getRight());
        String script = String.format("%s %s %s", left, binaryExpr.getOperator().getName(), right);

        params.add(new SQLCharExpr(script));

        return makeMethodField("script", params, null, null, true);
    }

    private static Object getScriptValue(SQLExpr expr) throws SqlParseException {
        if (expr instanceof SQLIdentifierExpr || expr instanceof SQLPropertyExpr || expr instanceof SQLVariantRefExpr) {
            return "doc['" + expr.toString() + "'].value";
        } else if (expr instanceof SQLValuableExpr) {
            return ((SQLValuableExpr) expr).getValue();
        }
        throw new SqlParseException("could not parse sqlBinaryOpExpr need to be identifier/valuable got" + expr.getClass().toString() + " with value:" + expr.toString());
    }

    private static Field handleIdentifier(SQLExpr expr, String alias, String tableAlias) throws SqlParseException {
        String name = expr.toString().replace("`", "");

        if (alias != null && alias != name) {
            List<SQLExpr> paramers = Lists.newArrayList();
            paramers.add(new SQLCharExpr(alias));
            paramers.add(new SQLCharExpr("doc['" + name + "'].value"));
            return makeMethodField("script", paramers, null, alias, true);
        }

        if (tableAlias == null) return new Field(name, alias);
        else if (tableAlias != null) {
            String aliasPrefix = tableAlias + ".";
            if (name.startsWith(aliasPrefix)) {
                name = name.replaceFirst(aliasPrefix, "");
                return new Field(name, alias);
            }
        }
        return null;
    }

    private static MethodField makeMethodField(String name, List<SQLExpr> arguments, SQLAggregateOption option, String alias, boolean first) throws SqlParseException {
        List<KVValue> paramers = new LinkedList<>();

        String finalMethodName = name;

        for (SQLExpr object : arguments) {
            if (object instanceof SQLBinaryOpExpr) {

                SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) object;
                if (!binaryOpExpr.getOperator().getName().equals("=")) {
                    paramers.add(new KVValue("script", makeScriptMethodField(binaryOpExpr, null)));
                } else {
                    SQLExpr right = binaryOpExpr.getRight();
                    Object value = Util.expr2Object(right);
                    paramers.add(new KVValue(binaryOpExpr.getLeft().toString(), value));
                }
            } else if (object instanceof SQLMethodInvokeExpr) {
                //method nest method
                SQLMethodInvokeExpr mExpr = (SQLMethodInvokeExpr) object;
                String methodName = mExpr.getMethodName().toLowerCase();
                if (methodName.equals("script")) {
                    KVValue script = new KVValue("script", makeMethodField(mExpr.getMethodName(), mExpr.getParameters(), null, alias, true));
                    paramers.add(script);
                } else if (methodName.equals("nested") || methodName.equals("reverse_nested")) {
                    NestedType nestedType = new NestedType();
                    if (!nestedType.tryFillFromExpr(object)) {
                        throw new SqlParseException("failed parsing nested expr " + object);
                    }
                    paramers.add(new KVValue("nested", nestedType));
                } else {
                    //throw new SqlParseException("only support script/nested as inner functions");
                    MethodField abc = makeMethodField(methodName, mExpr.getParameters(), null, null, false);
                    paramers.add(new KVValue(abc.getParams().get(0).toString(), abc.getParams().get(1)));

                }

            } else {
                paramers.add(new KVValue(Util.expr2Object(object)));
            }

        }

        //just check we can find the function
        if (SQLFunctions.function(finalMethodName, paramers, null) != null) {
            if (alias == null && first) {
                alias = paramers.get(0).value.toString();
            }
            Tuple<String, String> newFunctions = SQLFunctions.function(finalMethodName, paramers,
                    paramers.get(0).key);
            paramers.clear();
            if (!first) {
                paramers.add(new KVValue(newFunctions.v1()));
            } else {
                paramers.add(new KVValue(alias));
            }

            paramers.add(new KVValue(newFunctions.v2()));
            finalMethodName = "script";
        }


        return new MethodField(finalMethodName, paramers, option == null ? null : option.name(), alias);
    }

}
