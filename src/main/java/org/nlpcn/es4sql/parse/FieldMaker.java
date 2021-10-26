package org.nlpcn.es4sql.parse;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLAggregateOption;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLCaseExpr;
import com.alibaba.druid.sql.ast.expr.SQLCastExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.util.StringUtils;
import com.google.common.collect.Lists;
import org.elasticsearch.core.Tuple;
import org.nlpcn.es4sql.SQLFunctions;
import org.nlpcn.es4sql.Util;
import org.nlpcn.es4sql.domain.Field;
import org.nlpcn.es4sql.domain.KVValue;
import org.nlpcn.es4sql.domain.MethodField;
import org.nlpcn.es4sql.domain.Where;
import org.nlpcn.es4sql.exception.SqlParseException;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

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
            } else if ("filters".equalsIgnoreCase(methodName)) {
                return makeFiltersMethodField(mExpr, alias);
            } else if ("field_sort".equalsIgnoreCase(methodName)) {
                return makeFieldSortMethodField(mExpr, alias);
            }

            return makeMethodField(methodName, mExpr.getParameters(), null, alias, tableAlias, true);
        } else if (expr instanceof SQLAggregateExpr) {
            SQLAggregateExpr sExpr = (SQLAggregateExpr) expr;
            return makeMethodField(sExpr.getMethodName(), sExpr.getArguments(), sExpr.getOption(), alias, tableAlias, true);
        } else if (expr instanceof SQLCaseExpr) {
            //zhongshu-comment case when走这个分支
            String scriptCode = new CaseWhenParser((SQLCaseExpr) expr, alias, tableAlias).parse();
            List<KVValue> methodParameters = new ArrayList<>();
            /*zhongshu-comment group by子句中case when是没有别名的，这时alias=null，调用KVValue的toString()会报空指针
            methodParameters.add(new KVValue(alias)); //zhongshu-comment 这是原语句，被我注释掉了，改为下面带非空判断的语句*/
            if (null != alias && alias.trim().length() != 0) {
                methodParameters.add(new KVValue(alias));
            }
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

        //added by xzb 修复 if 条件的 value 被去除单引号问题
      /*  String tmp = binaryExpr.getRight().toString().trim();
        if (tmp.startsWith("'") && tmp.endsWith("'")) {
            right = "'" + right + "'";
        }*/

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

    private static Field makeFiltersMethodField(SQLMethodInvokeExpr filtersMethod, String alias) throws SqlParseException {
        List<SQLExpr> parameters = filtersMethod.getParameters();
        int firstFilterMethod = -1;
        int parametersSize = parameters.size();
        for (int i = 0; i < parametersSize; ++i) {
            if (parameters.get(i) instanceof SQLMethodInvokeExpr) {
                firstFilterMethod = i;
                break;
            }
        }
        if (firstFilterMethod < 0) {
            throw new SqlParseException("filters group by field should have one more filter methods");
        }

        String filtersAlias = filtersMethod.getMethodName();
        String otherBucketKey = null;
        if (0 < firstFilterMethod) {
            filtersAlias = Util.extendedToString(parameters.get(0));
            if (1 < firstFilterMethod) {
                otherBucketKey = Util.extendedToString(parameters.get(1));
            }
        }
        List<Field> filterFields = new ArrayList<>();
        for (SQLExpr expr : parameters.subList(firstFilterMethod, parametersSize)) {
            filterFields.add(makeFilterMethodField((SQLMethodInvokeExpr) expr, null));
        }
        List<KVValue> methodParameters = new ArrayList<>();
        methodParameters.add(new KVValue("alias", filtersAlias + "@FILTERS"));
        methodParameters.add(new KVValue("otherBucketKey", otherBucketKey));
        methodParameters.add(new KVValue("filters", filterFields));
        return new MethodField("filters", methodParameters, null, alias);
    }

    private static Field makeFieldSortMethodField(SQLMethodInvokeExpr fieldSortMethod, String alias) throws SqlParseException {
        String fieldName = null;
        List<SQLExpr> parameters = fieldSortMethod.getParameters();
        List<KVValue> methodParameters = new ArrayList<>();
        for (SQLExpr parameter : parameters) {
            if (parameter instanceof SQLIdentifierExpr) {
                fieldName = parameter.toString();
            } else if (parameter instanceof SQLBinaryOpExpr) {
                String key = ((SQLBinaryOpExpr) parameter).getLeft().toString();
                Object value = Util.expr2Object(((SQLBinaryOpExpr) parameter).getRight());
                if ("field".equals(key)) {
                    fieldName = value.toString();
                } else {
                    methodParameters.add(new KVValue(key, value));
                }
            } else {
                throw new SqlParseException("unknown parameter : " + parameter);
            }
        }

        if (fieldName == null) {
            throw new SqlParseException("field name not found");
        }

        methodParameters.add(new KVValue("field", fieldName));
        return new MethodField("field_sort", methodParameters, null, alias);
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
        if (alias != null && !alias.equals(name) && !Util.isFromJoinOrUnionTable(expr)) {

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
        //added by xzb 默认的二元操作符为 ==
        String binaryOperatorName = null;
        List<String> binaryOperatorNames = new ArrayList<>();
        for (SQLExpr object : arguments) {

            if (object instanceof SQLBinaryOpExpr) {

                SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) object;

                if (SQLFunctions.buildInFunctions.contains(binaryOpExpr.getOperator().toString().toLowerCase())) {
                    SQLMethodInvokeExpr mExpr = makeBinaryMethodField(binaryOpExpr, alias, first);
                    MethodField mf = makeMethodField(mExpr.getMethodName(), mExpr.getParameters(), null, null, tableAlias, false);
                    String key = mf.getParams().get(0).toString(), value = mf.getParams().get(1).toString();
                    paramers.add(new KVValue(key, new SQLCharExpr(first && !SQLFunctions.buildInFunctions.contains(finalMethodName) ? String.format("%s;return %s;", value, key) : value)));
                } else {
                  //modified by xzb 增加 =、!= 以外二元操作符的支持
                     binaryOperatorName = binaryOpExpr.getOperator().getName().trim();
                    if (SQLFunctions.binaryOperators.contains(binaryOperatorName)) {
                        binaryOperatorNames.add(binaryOperatorName);
                        SQLExpr right = binaryOpExpr.getRight();

                        Object value = Util.expr2Object(right);

                        //added by xzb if 语法的二元操作符的值如果有引号，不能去掉
                        //select  name, if(gender='m','男','女') as myGender from bank  LIMIT 0, 10
                        if (binaryOpExpr.getParent() instanceof SQLMethodInvokeExpr) {
                            String  methodName  = ((SQLMethodInvokeExpr)binaryOpExpr.getParent()).getMethodName();
                            if ("if".equals(methodName) || "case".equals(methodName) || "case_new".equals(methodName)) {
                                value = Util.expr2Object(right, "'");
                            }
                        }
                        paramers.add(new KVValue(binaryOpExpr.getLeft().toString(), value));
                    } else {
                        paramers.add(new KVValue("script", makeScriptMethodField(binaryOpExpr, null, tableAlias)));
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
                    //added by xzb 2020-05-07 用于聚合查询时支持if、case_new 函数生成新的值
                    if (mExpr.getParent() instanceof  SQLAggregateExpr) {
                        KVValue script = new KVValue("script", makeMethodField(mExpr.getMethodName(), mExpr.getParameters(), null, alias, tableAlias, true));
                        paramers.add(script);
                    } else {
                        MethodField mf = makeMethodField(methodName, mExpr.getParameters(), null, null, tableAlias, false);
                        String key = mf.getParams().get(0).toString(), value = mf.getParams().get(1).toString();
                        paramers.add(new KVValue(key, new SQLCharExpr(first && !SQLFunctions.buildInFunctions.contains(finalMethodName) ? String.format("%s;return %s;", value, key) : value)));
                    }

                } else throw new SqlParseException("only support script/nested/children as inner functions");
            } else if (object instanceof SQLCaseExpr) {
                String scriptCode = new CaseWhenParser((SQLCaseExpr) object, alias, tableAlias).parse();
                paramers.add(new KVValue("script",new SQLCharExpr(scriptCode)));
            } else if(object instanceof SQLCastExpr) {
                CastParser castParser = new CastParser((SQLCastExpr) object, alias, tableAlias);
                String scriptCode = castParser.parse(false);
                paramers.add(new KVValue(castParser.getName(),new SQLCharExpr(scriptCode)));
            } else {
                paramers.add(new KVValue(Util.removeTableAilasFromField(object, tableAlias)));
            }

        }
        //zhongshu-comment script字段不会走这个分支
        //just check we can find the function
        if (SQLFunctions.buildInFunctions.contains(finalMethodName.toLowerCase())) {
            if (alias == null && first) {
                alias = "field_" + SQLFunctions.random();//paramers.get(0).value.toString();
            }

            List<KVValue> nestedTypes = new ArrayList<>();
            for (ListIterator<KVValue> it = paramers.listIterator(); it.hasNext(); ) {
                KVValue param = it.next();
                if ("nested".equals(param.key)) {
                    NestedType nestedType = (NestedType) param.value;
                    it.set(new KVValue(new SQLCharExpr(nestedType.field)));
                    nestedTypes.add(param);
                }
            }

            //should check if field and first .
            Tuple<String, String> newFunctions = null;
            try {
                //added by xzb 构造script时，二元操作符可能是多样的 case_new 语法，需要 binaryOperatorNames 参数
                newFunctions = SQLFunctions.function(finalMethodName, paramers, !paramers.isEmpty() ? paramers.get(0).key : null,first, binaryOperatorName, binaryOperatorNames);
            } catch (Exception e) {
                e.printStackTrace();
            }
            paramers.clear();
            if (!first) {
                //variance
                paramers.add(new KVValue(newFunctions.v1()));
            } else {
                
                if(newFunctions.v1().toLowerCase().contains("if")){
                    //added by xzb 如果有用户指定的别名，则不使用自动生成的别名
                    if (!StringUtils.isEmpty(alias) && !alias.startsWith("field_")) {
                        paramers.add(new KVValue(alias));
                    } else {
                        paramers.add(new KVValue(newFunctions.v1()));
                    }
                }else {
                    paramers.add(new KVValue(alias));
                }
            }

            paramers.add(new KVValue(newFunctions.v2()));
            paramers.addAll(nestedTypes);
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
