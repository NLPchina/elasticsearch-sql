package org.nlpcn.es4sql.parse;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLTextLiteralExpr;
import org.nlpcn.es4sql.Util;
import org.nlpcn.es4sql.domain.Where;
import org.nlpcn.es4sql.exception.SqlParseException;

import java.util.List;

/**
 * Created by Eliran on 12/11/2015.
 */
public class NestedType {
    public String field;
    public String path;
    public Where where;
    private boolean simple;

    public boolean tryFillFromExpr(SQLExpr expr) throws SqlParseException {
        if (!(expr instanceof SQLMethodInvokeExpr)) return false;
        SQLMethodInvokeExpr method = (SQLMethodInvokeExpr) expr;
        if (!method.getMethodName().toLowerCase().equals("nested")) return false;

        List<SQLExpr> parameters = method.getParameters();
        if (parameters.size() != 2 && parameters.size() != 1)
            throw new SqlParseException("on nested object only allowed 2 parameters (field,path)/(path,conditions..) or 1 parameter (field) ");

        String field = Util.extendedToString(parameters.get(0));
        this.field = field;
        if (parameters.size() == 1) {
            //calc path myself..
            if (!field.contains("."))
                throw new SqlParseException("nested should contain . on their field name");
            int lastDot = field.lastIndexOf(".");
            this.path = field.substring(0, lastDot);
            this.simple = true;
        } else if (parameters.size() == 2) {
            SQLExpr secondParameter = parameters.get(1);
            if(secondParameter instanceof SQLTextLiteralExpr || secondParameter instanceof SQLIdentifierExpr || secondParameter instanceof SQLPropertyExpr) {
                this.path = Util.extendedToString(secondParameter);
                this.simple = true;
            }
            else {
                this.path = field;
                Where where = Where.newInstance();
                new SqlParser().parseWhere(secondParameter,where);
                if(where.getWheres().size() == 0)
                    throw new SqlParseException("unable to parse filter where.");
                this.where = where;
                simple = false;
            }
        }

        return true;
    }

    public boolean isSimple() {
        return simple;
    }
}
