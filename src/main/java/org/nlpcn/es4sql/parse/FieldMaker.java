package org.nlpcn.es4sql.parse;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.alibaba.druid.sql.ast.expr.*;
import com.google.common.collect.Lists;
import org.elasticsearch.common.collect.Tuple;
import org.nlpcn.es4sql.SQLFunctions;
import org.nlpcn.es4sql.Util;
import org.nlpcn.es4sql.domain.Field;
import org.nlpcn.es4sql.domain.KVValue;
import org.nlpcn.es4sql.domain.MethodField;
import org.nlpcn.es4sql.domain.Where;
import org.nlpcn.es4sql.exception.SqlParseException;
import com.alibaba.druid.sql.ast.*;

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
            return makeField(makeBinaryMethodField((SQLBinaryOpExpr) expr, alias, true), alias, tableAlias);

        } else if (expr instanceof SQLAllColumnExpr) {//zhongshu-comment 对应select * 的情况
        } else if (expr instanceof SQLMethodInvokeExpr) {
            SQLMethodInvokeExpr mExpr = (SQLMethodInvokeExpr) expr;

            String methodName = mExpr.getMethodName();

            if (methodName.equalsIgnoreCase("nested") || methodName.equalsIgnoreCase("reverse_nested")) {
                NestedType nestedType = new NestedType();
                if (nestedType.tryFillFromExpr(mExpr)) {
                    return handleIdentifier(nestedType, alias, tableAlias);
                }
            } else if (methodName.equalsIgnoreCase("children")) {
                ChildrenType childrenType = new ChildrenType();
                if (childrenType.tryFillFromExpr(mExpr)) {
                    return handleIdentifier(childrenType, alias, tableAlias);
                }
            } else if (methodName.equalsIgnoreCase("filter")) {
                return makeFilterMethodField(mExpr, alias);
            }

            return makeMethodField(methodName, mExpr.getParameters(), null, alias, tableAlias, true);
        } else if (expr instanceof SQLAggregateExpr) {
            SQLAggregateExpr sExpr = (SQLAggregateExpr) expr;
            return makeMethodField(sExpr.getMethodName(), sExpr.getArguments(), sExpr.getOption(), alias, tableAlias, true);
        } else if (expr instanceof SQLCaseExpr) {
            //zhongshu-comment case when走这个分支
            String scriptCode = new CaseWhenParser((SQLCaseExpr) expr, alias, tableAlias).parse();
            List<KVValue> methodParameters = new ArrayList<>();
            //zhongshu-comment group by子句中case when是没有别名的，这时alias=null，调用KVValue的toString()会报空指针
            methodParameters.add(new KVValue(alias));
            methodParameters.add(new KVValue(scriptCode));
            return new MethodField("script", methodParameters, null, alias);
        }else if (expr instanceof SQLCastExpr) {
            SQLCastExpr castExpr = (SQLCastExpr) expr;
            if (alias == null) {
                alias = "cast_" + castExpr.getExpr().toString();
            }
            String scriptCode = new CastParser(castExpr, alias, tableAlias).parse(true);
            List<KVValue> methodParameters = new ArrayList<>();
            methodParameters.add(new KVValue(alias));
            methodParameters.add(new KVValue(scriptCode));
            return new MethodField("script", methodParameters, null, alias);
        } else {
            throw new SqlParseException("unknown field name : " + expr);
        }
        return null;
    }

    private static Object getScriptValue(SQLExpr expr) throws SqlParseException {
        return Util.getScriptValue(expr);
    }

    private static Field makeScriptMethodField(SQLBinaryOpExpr binaryExpr, String alias, String tableAlias) throws SqlParseException {
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

        return makeMethodField("script", params, null, null, tableAlias, false);
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
        new WhereParser(new SqlParser()).parseWhere(exprToCheck, where);
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
        field.setChildren(null);
        return field;
    }

    private static Field handleIdentifier(ChildrenType childrenType, String alias, String tableAlias) throws SqlParseException {
        Field field = handleIdentifier(new SQLIdentifierExpr(childrenType.field), alias, tableAlias);
        field.setNested(null);
        field.setChildren(childrenType);
        return field;
    }


    //binary method can nested
    public static SQLMethodInvokeExpr makeBinaryMethodField(SQLBinaryOpExpr expr, String alias, boolean first) throws SqlParseException {
        List<SQLExpr> params = new ArrayList<>();

        String scriptFieldAlias;
        if (first && (alias == null || alias.equals(""))) {
            scriptFieldAlias = "field_" + SQLFunctions.random();
        } else {
            scriptFieldAlias = alias;
        }
        params.add(new SQLCharExpr(scriptFieldAlias));

        switch (expr.getOperator()) {
            case Add:
                return convertBinaryOperatorToMethod("add", expr);
            case Multiply:
                return convertBinaryOperatorToMethod("multiply", expr);

            case Divide:
                return convertBinaryOperatorToMethod("divide", expr);

            case Modulus:
                return convertBinaryOperatorToMethod("modulus", expr);

            case Subtract:
                return convertBinaryOperatorToMethod("subtract", expr);
            default:
                throw new SqlParseException(expr.getOperator().getName() + " is not support");
        }
    }

    private static SQLMethodInvokeExpr convertBinaryOperatorToMethod(String operator, SQLBinaryOpExpr expr) {
        SQLMethodInvokeExpr methodInvokeExpr = new SQLMethodInvokeExpr(operator, null);
        methodInvokeExpr.addParameter(expr.getLeft());
        methodInvokeExpr.addParameter(expr.getRight());
        return methodInvokeExpr;
    }


    private static Field handleIdentifier(SQLExpr expr, String alias, String tableAlias) throws SqlParseException {
        String name = expr.toString().replace("`", "");
        String newFieldName = name;
        Field field = null;
        if (tableAlias != null) {
            String aliasPrefix = tableAlias + ".";
            if (name.startsWith(aliasPrefix)) {
                newFieldName = name.replaceFirst(aliasPrefix, "");
                field = new Field(newFieldName, alias);
            }
        }

        if (tableAlias == null) {
            field = new Field(newFieldName, alias);
        }

        //zhongshu-comment 字段的别名不为空 && 别名和字段名不一样
        //zhongshu-comment bug，应该改为 !alias.equals(name)，
        if (alias != null && alias != name && !Util.isFromJoinOrUnionTable(expr)) {

            /*
            zhongshu-comment newFieldName是字段原来的名字，这句话应该是用于es dsl的
            使用别名有很多种情况：
                1、最简单的就是select field_1 as a from tbl
                2、调用函数处理字段之后，select floor(field_1) as a from tbl
                3、执行表达式，select field_1 + field_2 as a from tbl
                4、case when field_1='a' then 'haha' else 'hehe' end as a
                5、........
            所以这个if分支就是为了处理以上这些情况的
             */
            List<SQLExpr> paramers = Lists.newArrayList();
            paramers.add(new SQLCharExpr(alias)); //zhongshu-comment 别名
            paramers.add(new SQLCharExpr("doc['" + newFieldName + "'].value"));
            field = makeMethodField("script", paramers, null, alias, tableAlias, true);
        }
        return field;
    }

    public static MethodField makeMethodField(String name, List<SQLExpr> arguments, SQLAggregateOption option, String alias, String tableAlias, boolean first) throws SqlParseException {
        List<KVValue> paramers = new LinkedList<>();
        String finalMethodName = name;

        for (SQLExpr object : arguments) {

            if (object instanceof SQLBinaryOpExpr) {

                SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) object;

                if (SQLFunctions.buildInFunctions.contains(binaryOpExpr.getOperator().toString().toLowerCase())) {
                    SQLMethodInvokeExpr mExpr = makeBinaryMethodField(binaryOpExpr, alias, first);
                    MethodField abc = makeMethodField(mExpr.getMethodName(), mExpr.getParameters(), null, null, tableAlias, false);
                    paramers.add(new KVValue(abc.getParams().get(0).toString(), new SQLCharExpr(abc.getParams().get(1).toString())));
                } else {
                    if (!binaryOpExpr.getOperator().getName().equals("=")) {
                        paramers.add(new KVValue("script", makeScriptMethodField(binaryOpExpr, null, tableAlias)));
                    } else {
                        SQLExpr right = binaryOpExpr.getRight();
                        Object value = Util.expr2Object(right);
                        paramers.add(new KVValue(binaryOpExpr.getLeft().toString(), value));
                    }
                }

            } else if (object instanceof SQLMethodInvokeExpr) {
                SQLMethodInvokeExpr mExpr = (SQLMethodInvokeExpr) object;
                String methodName = mExpr.getMethodName().toLowerCase();
                if (methodName.equals("script")) {
                    KVValue script = new KVValue("script", makeMethodField(mExpr.getMethodName(), mExpr.getParameters(), null, alias, tableAlias, true));
                    paramers.add(script);
                } else if (methodName.equals("nested") || methodName.equals("reverse_nested")) {
                    NestedType nestedType = new NestedType();

                    if (!nestedType.tryFillFromExpr(object)) {
                        throw new SqlParseException("failed parsing nested expr " + object);
                    }

                    paramers.add(new KVValue("nested", nestedType));
                } else if (methodName.equals("children")) {
                    ChildrenType childrenType = new ChildrenType();

                    if (!childrenType.tryFillFromExpr(object)) {
                        throw new SqlParseException("failed parsing children expr " + object);
                    }

                    paramers.add(new KVValue("children", childrenType));
                } else if (SQLFunctions.buildInFunctions.contains(methodName)) {
                    //throw new SqlParseException("only support script/nested as inner functions");
                    MethodField abc = makeMethodField(methodName, mExpr.getParameters(), null, null, tableAlias, false);
                    paramers.add(new KVValue(abc.getParams().get(0).toString(), new SQLCharExpr(abc.getParams().get(1).toString())));
                } else throw new SqlParseException("only support script/nested/children as inner functions");
            } else if (object instanceof SQLCaseExpr) {
                String scriptCode = new CaseWhenParser((SQLCaseExpr) object, alias, tableAlias).parse();
                paramers.add(new KVValue("script",new SQLCharExpr(scriptCode)));
            } else if(object instanceof SQLCastExpr) {
                String scriptCode = new CastParser((SQLCastExpr) object, alias, tableAlias).parse(false);
                paramers.add(new KVValue("script",new SQLCharExpr(scriptCode)));
            } else {
                paramers.add(new KVValue(Util.removeTableAilasFromField(object, tableAlias)));
            }

        }
        //zhongshu-comment script字段不会走这个分支
        //just check we can find the function
        if (SQLFunctions.buildInFunctions.contains(finalMethodName)) {
            if (alias == null && first) {
                alias = "field_" + SQLFunctions.random();//paramers.get(0).value.toString();
            }
            //should check if field and first .
            Tuple<String, String> newFunctions = SQLFunctions.function(finalMethodName, paramers,
                    paramers.get(0).key,first);
            paramers.clear();
            if (!first) {
                //variance
                paramers.add(new KVValue(newFunctions.v1()));
            } else {
                paramers.add(new KVValue(alias));
            }

            paramers.add(new KVValue(newFunctions.v2()));
            finalMethodName = "script";
        }
        if (first) {
            List<KVValue> tempParamers = new LinkedList<>();
            for (KVValue temp : paramers) {
                if (temp.value instanceof SQLExpr)
                    tempParamers.add(new KVValue(temp.key, Util.expr2Object((SQLExpr) temp.value)));
                else tempParamers.add(new KVValue(temp.key, temp.value));
            }
            paramers.clear();
            paramers.addAll(tempParamers);
        }

        return new MethodField(finalMethodName, paramers, option == null ? null : option.name(), alias);
    }
}
