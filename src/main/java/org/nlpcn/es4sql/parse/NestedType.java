package org.nlpcn.es4sql.parse;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import org.nlpcn.es4sql.exception.SqlParseException;

import java.util.List;

/**
 * Created by Eliran on 12/11/2015.
 */
public class NestedType {
    public String field;
    public String path;

    public boolean tryFillFromExpr(SQLExpr expr) throws SqlParseException {
        if (!(expr instanceof SQLMethodInvokeExpr)) return false;
        SQLMethodInvokeExpr method = (SQLMethodInvokeExpr) expr;
        if (!method.getMethodName().toLowerCase().equals("nested")) return false;

        List<SQLExpr> parameters = method.getParameters();
        if (parameters.size() != 2 && parameters.size() != 1)
            throw new SqlParseException("on nested object only allowed 2 parameters (field,path) or 1 parameter (field) ");

        String field = parameters.get(0).toString();
        this.field = field;
        if (parameters.size() == 1) {
            //calc path myself..
            if (!field.contains("."))
                throw new SqlParseException("nested should contain . on their field name");
            int lastDot = field.lastIndexOf(".");
            this.path = field.substring(0, lastDot);
        } else if (parameters.size() == 2) {
            this.path = parameters.get(1).toString();
        }

        return true;
    }
}
