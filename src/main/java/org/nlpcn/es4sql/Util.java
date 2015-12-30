package org.nlpcn.es4sql;

import java.util.List;
import java.util.Map;

import com.alibaba.druid.sql.ast.expr.*;
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

	public static Object expr2Object(SQLExpr expr) throws SqlParseException {
		Object value = null;
		if (expr instanceof SQLNumericLiteralExpr) {
			value = ((SQLNumericLiteralExpr) expr).getNumber();
		} else if (expr instanceof SQLCharExpr) {
			value = ((SQLCharExpr) expr).getText();
		} else if (expr instanceof SQLIdentifierExpr) {
			value = expr.toString();
		} else if (expr instanceof SQLPropertyExpr) {
            value = expr.toString();
        }else if (expr instanceof SQLVariantRefExpr ){
            value = expr.toString();
		}else if (expr instanceof SQLAllColumnExpr) {
			value = "*";
		} else if (expr instanceof  SQLValuableExpr){
            value = ((SQLValuableExpr)expr).getValue();
        } else {
			throw new SqlParseException("can not support this type " + expr.getClass());
		}
		return value;
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
        if(sqlExpr instanceof SQLTextLiteralExpr){
            return ((SQLTextLiteralExpr) sqlExpr).getText();
        }
        return sqlExpr.toString();
    }

    public static String[] concatStringsArrays(String[] a1,String[] a2){
        String[] strings = new String[a1.length + a2.length];
        for(int i=0;i<a1.length;i++){
            strings[i] = a1[i];
        }
        for(int i = 0;i<a2.length;i++){
            strings[a1.length+i] = a2[i];
        }
        return strings;
    }
}
