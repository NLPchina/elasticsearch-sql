package org.nlpcn.es4sql.parse;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import org.nlpcn.es4sql.Util;
import org.nlpcn.es4sql.domain.Field;
import org.nlpcn.es4sql.domain.KVValue;
import org.nlpcn.es4sql.exception.SqlParseException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Eliran on 11/12/2015.
 */
public class ScriptFilter {
    private String script;
    private Map<String,Object> args;

    public ScriptFilter() {
        args = null;
    }

    public ScriptFilter(String script, Map<String, Object> args) {
        this.script = script;
        this.args = args;
    }

    public boolean tryParseFromMethodExpr(SQLMethodInvokeExpr expr) throws SqlParseException {
        if (!expr.getMethodName().toLowerCase().equals("script")) {
            return false;
        }
        List<SQLExpr> methodParameters = expr.getParameters();
        if (methodParameters.size() == 0) {
            return false;
        }
        script = Util.extendedToString(methodParameters.get(0));

        if (methodParameters.size() == 1) {
            return true;
        }

        args = new HashMap<>();
        for (int i = 1; i < methodParameters.size(); i++) {

            SQLExpr innerExpr = methodParameters.get(i);
            if (!(innerExpr instanceof SQLBinaryOpExpr)) {
                return false;
            }
            SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) innerExpr;
            if (!binaryOpExpr.getOperator().getName().equals("=")) {
                return false;
            }

            SQLExpr right = binaryOpExpr.getRight();
            Object value = Util.expr2Object(right);
            String key = Util.extendedToString(binaryOpExpr.getLeft());
            args.put(key, value);

        }
        return true;
    }

    public boolean containsParameters(){
        return args!=null && args.size() > 0;
    }

    public String getScript() {
        return script;
    }


    public Map<String, Object> getArgs() {
        return args;
    }

}
