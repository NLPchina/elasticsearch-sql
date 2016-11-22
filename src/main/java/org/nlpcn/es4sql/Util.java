package org.nlpcn.es4sql;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUnionQuery;
import org.nlpcn.es4sql.domain.Field;
import org.nlpcn.es4sql.domain.KVValue;
import org.nlpcn.es4sql.exception.SqlParseException;

import com.alibaba.druid.sql.ast.*;


public class Util {
    public static String joiner(List<KVValue> lists, String oper) {

        if (lists.size() == 0) {
            return null;
        }

        StringBuilder sb = new StringBuilder(lists.get(0).toString());
        for (int i = 1; i < lists.size(); i++) {
            sb.append(oper);
            sb.append(lists.get(i).toString());
        }

        return sb.toString();
    }

    public static List<Map<String, Object>> sortByMap(List<Map<String, Object>> lists) {

        return lists;
    }

    public static Object removeTableAilasFromField(Object expr, String tableAlias) {

        if (expr instanceof SQLIdentifierExpr || expr instanceof SQLPropertyExpr || expr instanceof SQLVariantRefExpr) {
            String name = expr.toString().replace("`", "");
            if (tableAlias != null) {
                String aliasPrefix = tableAlias + ".";
                if (name.startsWith(aliasPrefix)) {
                    String newFieldName = name.replaceFirst(aliasPrefix, "");
                    return new SQLIdentifierExpr(newFieldName);
                }
            }
        }
        return expr;
    }


    public static Object expr2Object(SQLExpr expr) {
        return expr2Object(expr, "");
    }

    public static Object expr2Object(SQLExpr expr, String charWithQuote) {
        Object value = null;
        if (expr instanceof SQLNumericLiteralExpr) {
            value = ((SQLNumericLiteralExpr) expr).getNumber();
        } else if (expr instanceof SQLCharExpr) {
            value = charWithQuote + ((SQLCharExpr) expr).getText() + charWithQuote;
        } else if (expr instanceof SQLIdentifierExpr) {
            value = expr.toString();
        } else if (expr instanceof SQLPropertyExpr) {
            value = expr.toString();
        } else if (expr instanceof SQLVariantRefExpr) {
            value = expr.toString();
        } else if (expr instanceof SQLAllColumnExpr) {
            value = "*";
        } else if (expr instanceof SQLValuableExpr) {
            value = ((SQLValuableExpr) expr).getValue();
        } else {
            //throw new SqlParseException("can not support this type " + expr.getClass());
        }
        return value;
    }

    public static Object getScriptValue(SQLExpr expr) throws SqlParseException {
        if (expr instanceof SQLIdentifierExpr || expr instanceof SQLPropertyExpr || expr instanceof SQLVariantRefExpr) {
            return "doc['" + expr.toString() + "'].value";
        } else if (expr instanceof SQLValuableExpr) {
            return ((SQLValuableExpr) expr).getValue();
        }
        throw new SqlParseException("could not parse sqlBinaryOpExpr need to be identifier/valuable got" + expr.getClass().toString() + " with value:" + expr.toString());
    }

    public static Object getScriptValueWithQuote(SQLExpr expr, String quote) throws SqlParseException {
        if (expr instanceof SQLIdentifierExpr || expr instanceof SQLPropertyExpr || expr instanceof SQLVariantRefExpr) {
            return "doc['" + expr.toString() + "'].value";
        }  else if (expr instanceof SQLCharExpr) {
            return quote + ((SQLCharExpr) expr).getValue() + quote;
        } else if (expr instanceof SQLIntegerExpr) {
            return ((SQLIntegerExpr) expr).getValue();
        } else if (expr instanceof SQLNumericLiteralExpr) {
            return ((SQLNumericLiteralExpr) expr).getNumber();
        } else if (expr instanceof SQLNullExpr ){
        	return ((SQLNullExpr) expr).toString().toLowerCase();
        }
        throw new SqlParseException("could not parse sqlBinaryOpExpr need to be identifier/valuable got" + expr.getClass().toString() + " with value:" + expr.toString());
    }

    public static boolean isFromJoinOrUnionTable(SQLExpr expr) {
        SQLObject temp = expr;
        AtomicInteger counter = new AtomicInteger(10);
        while (temp != null &&
                !(expr instanceof SQLSelectQueryBlock) &&
                !(expr instanceof SQLJoinTableSource) && !(expr instanceof SQLUnionQuery) && counter.get() > 0) {
            counter.decrementAndGet();
            temp = temp.getParent();
            if (temp instanceof SQLSelectQueryBlock) {
                SQLTableSource from = ((SQLSelectQueryBlock) temp).getFrom();
                if (from instanceof SQLJoinTableSource || from instanceof SQLUnionQuery) {
                    return true;
                }
            }
            if (temp instanceof SQLJoinTableSource || temp instanceof SQLUnionQuery) {
                return true;
            }
        }
        return false;
    }

    public static double[] String2DoubleArr(String paramer) {
        String[] split = paramer.split(",");
        double[] ds = new double[split.length];
        for (int i = 0; i < ds.length; i++) {
            ds[i] = Double.parseDouble(split[i].trim());
        }
        return ds;
    }

    public static double[] KV2DoubleArr(List<KVValue> params) {
        double[] ds = new double[params.size()];
        int i = 0;
        for (KVValue v : params) {
            ds[i] = ((Number) v.value).doubleValue();
            i++;
        }
        return ds;
    }


    public static String extendedToString(SQLExpr sqlExpr) {
        if (sqlExpr instanceof SQLTextLiteralExpr) {
            return ((SQLTextLiteralExpr) sqlExpr).getText();
        }
        return sqlExpr.toString();
    }

    public static String[] concatStringsArrays(String[] a1, String[] a2) {
        String[] strings = new String[a1.length + a2.length];
        for (int i = 0; i < a1.length; i++) {
            strings[i] = a1[i];
        }
        for (int i = 0; i < a2.length; i++) {
            strings[a1.length + i] = a2[i];
        }
        return strings;
    }

    public static Object searchPathInMap(Map<String, Object> fieldsMap, String[] path) {
        Map<String,Object> currentObject = fieldsMap;
        for(int i=0;i<path.length-1 ;i++){
            Object valueFromCurrentMap = currentObject.get(path[i]);
            if(valueFromCurrentMap == null) return null;
            if(!Map.class.isAssignableFrom(valueFromCurrentMap.getClass())) return null;
            currentObject = (Map<String, Object>) valueFromCurrentMap;
        }
        return currentObject.get(path[path.length-1]);
    }

    public static Object deepSearchInMap(Map<String,Object> fieldsMap , String field){
        if(field.contains(".")){
            String[] split = field.split("\\.");
            return searchPathInMap(fieldsMap,split);
        }
        return fieldsMap.get(field);
    }

    public static boolean clearEmptyPaths(Map<String, Object> map) {
        if(map.size() == 0){
            return true;
        }
        Set<String> keysToDelete = new HashSet<>();
        for (Map.Entry<String,Object> entry : map.entrySet()){
            Object value = entry.getValue();
            if(Map.class.isAssignableFrom(value.getClass())){
                if(clearEmptyPaths((Map<String, Object>) value)){
                    keysToDelete.add(entry.getKey());
                }
            }
        }
        if(keysToDelete.size() != 0){
            if(map.size() == keysToDelete.size()){
                map.clear();
                return true;
            }
            for(String key : keysToDelete){
                map.remove(key);
                return false;
            }
        }
        return false;
    }

}
