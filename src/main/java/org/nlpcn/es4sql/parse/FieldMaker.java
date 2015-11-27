package org.nlpcn.es4sql.parse;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.alibaba.druid.sql.ast.expr.*;
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
 *
 */
public class FieldMaker {
	public static Field makeField(SQLExpr expr, String alias,String tableAlias) throws SqlParseException {
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
            if(methodName.toLowerCase().equals("nested")){
                NestedType nestedType = new NestedType();
                if(nestedType.tryFillFromExpr(mExpr)){
                    return handleIdentifier(nestedType,alias,tableAlias);
                }
            }
            else  if (methodName.toLowerCase().equals("filter")){
                return makeFilterMethodField(mExpr,alias);
            }
            return makeMethodField(methodName, mExpr.getParameters(), null, alias);
		} else if (expr instanceof SQLAggregateExpr) {
			SQLAggregateExpr sExpr = (SQLAggregateExpr) expr;
			return makeMethodField(sExpr.getMethodName(), sExpr.getArguments(), sExpr.getOption(), alias);
		} else {
			throw new SqlParseException("unknow field name : " + expr);
		}
		return null;
	}

    private static Field makeFilterMethodField(SQLMethodInvokeExpr filterMethod,String alias) throws SqlParseException {
        List<SQLExpr> parameters = filterMethod.getParameters();
        int parametersSize = parameters.size();
        if(parametersSize != 1  && parametersSize !=2){
            throw new SqlParseException("filter group by field should only have one or 2 parameters filter(Expr) or filter(name,Expr)");
        }
        String filterAlias = filterMethod.getMethodName();
        SQLExpr exprToCheck = null;
        if(parametersSize == 1){
            exprToCheck = parameters.get(0);
            filterAlias = "filter(" + exprToCheck.toString().replaceAll("\n"," ") +")";
        }
        if(parametersSize == 2){
            //todo: function extendedToString - if sqlString remove ''
            filterAlias = extendedToString(parameters.get(0));
            exprToCheck = parameters.get(1);
        }
        Where where = Where.newInstance();
        new SqlParser().parseWhere(exprToCheck,where);
        if(where.getWheres().size() == 0)
            throw new SqlParseException("unable to parse filter where.");
        List<KVValue> methodParameters = new ArrayList<>();
        methodParameters.add(new KVValue("where",where));
        methodParameters.add(new KVValue("alias",filterAlias+"@FILTER"));
        return  new MethodField("filter", methodParameters,  null, alias);
    }

    private static String extendedToString(SQLExpr sqlExpr) {
        if(sqlExpr instanceof SQLTextLiteralExpr){
            return ((SQLTextLiteralExpr) sqlExpr).getText();
        }
        return sqlExpr.toString();
    }

    private static Field handleIdentifier(NestedType nestedType, String alias, String tableAlias) {
        Field field = handleIdentifier(new SQLIdentifierExpr(nestedType.field), alias, tableAlias);
        field.setNested(true);
        field.setNestedPath(nestedType.path);
        return field;
    }

    private static Field makeScriptMethodField(SQLBinaryOpExpr binaryExpr, String alias) throws SqlParseException {
        List<SQLExpr> params = new ArrayList<>();

        String scriptFieldAlias;
        if(alias == null || alias.equals(""))
            scriptFieldAlias = binaryExpr.toString();
        else
            scriptFieldAlias = alias;
        params.add(new SQLCharExpr(scriptFieldAlias));

        Object left = getScriptValue(binaryExpr.getLeft());
        Object right = getScriptValue(binaryExpr.getRight());
        String script = String.format("%s %s %s" , left ,binaryExpr.getOperator().getName() , right);

        params.add(new SQLCharExpr(script));

        return makeMethodField("script",params,null,null);
    }

    private static Object getScriptValue(SQLExpr expr) throws SqlParseException {
        if (expr instanceof SQLIdentifierExpr || expr instanceof SQLPropertyExpr || expr instanceof SQLVariantRefExpr) {
            return "doc['" + expr.toString() + "'].value";
        }
        else if (expr instanceof SQLValuableExpr){
            return ((SQLValuableExpr)expr).getValue();
        }
        throw new SqlParseException("could not parse sqlBinaryOpExpr need to be identifier/valuable got" + expr.getClass().toString() + " with value:" +expr.toString() );
    }

    private static Field handleIdentifier(SQLExpr expr, String alias, String tableAlias) {
        String name = expr.toString().replace("`", "");
        if(tableAlias==null) return new Field(name, alias);
        else if(tableAlias!=null){
            String aliasPrefix = tableAlias + ".";
            if(name.startsWith(aliasPrefix))
            {
                name = name.replaceFirst(aliasPrefix,"");
                return new Field(name, alias);
            }
        }
        return null;
    }

    private static MethodField makeMethodField(String name, List<SQLExpr> arguments, SQLAggregateOption option, String alias) throws SqlParseException {
		List<KVValue> paramers = new LinkedList<>();
		for (SQLExpr object : arguments) {
            if (object instanceof SQLBinaryOpExpr) {

                SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) object;
                if(!binaryOpExpr.getOperator().getName().equals("=")){
                    paramers.add(new KVValue("script", makeScriptMethodField(binaryOpExpr,null)));
                }
                else {
                    SQLExpr right = binaryOpExpr.getRight();
                    Object value = Util.expr2Object(right);
                    paramers.add(new KVValue(binaryOpExpr.getLeft().toString(), value));
                }
            } else if(object instanceof SQLMethodInvokeExpr) {
                SQLMethodInvokeExpr mExpr = (SQLMethodInvokeExpr) object;
                if(mExpr.getMethodName().toLowerCase().equals("script")){
                    KVValue script = new KVValue("script", makeMethodField(mExpr.getMethodName(), mExpr.getParameters(), null, alias));
                    paramers.add(script);
                }
                else throw new SqlParseException("only support script as nested functions");
            }else {
				paramers.add(new KVValue(Util.expr2Object(object)));
			}

		}
		return new MethodField(name, paramers, option == null ? null : option.name(), alias);
	}
}
