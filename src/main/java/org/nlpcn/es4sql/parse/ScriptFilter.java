package org.nlpcn.es4sql.parse;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
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
    private ScriptType scriptType;
    public ScriptFilter() {

        args = null;
        scriptType = ScriptType.INLINE;
    }

    public ScriptFilter(String script, Map<String, Object> args, ScriptType scriptType) {
        this.script = script;
        this.args = args;
        this.scriptType = scriptType;
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
            if(key.equals("script_type")){
                parseAndUpdateScriptType(value.toString());
            }
            else {
                args.put(key, value);
            }

        }
        return true;
    }

    private void parseAndUpdateScriptType(String scriptType) {
        String scriptTypeUpper = scriptType.toUpperCase();
        switch(scriptTypeUpper){
            case "INLINE":
                this.scriptType = ScriptType.INLINE;
                break;
            case "INDEXED":
            case "STORED":
                this.scriptType = ScriptType.STORED;
                break;
        }
    }

    public boolean containsParameters(){
        return args!=null && args.size() > 0;
    }

    public String getScript() {
        return script;
    }

    public ScriptType getScriptType() {
        return scriptType;
    }

    public Map<String, Object> getArgs() {
        return args;
    }

}
