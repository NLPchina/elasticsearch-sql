package org.nlpcn.es4sql.parse;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLTextLiteralExpr;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.query.NestedQueryBuilder;
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
    private boolean reverse;
    private boolean simple;

    private String innerHits;
    private ScoreMode scoreMode = ScoreMode.None;

    public boolean tryFillFromExpr(SQLExpr expr) throws SqlParseException {
        if (!(expr instanceof SQLMethodInvokeExpr)) return false;
        SQLMethodInvokeExpr method = (SQLMethodInvokeExpr) expr;
        String methodNameLower = method.getMethodName().toLowerCase();
        if (!(methodNameLower.equals("nested") || methodNameLower.equals("reverse_nested"))) return false;

        reverse = methodNameLower.equals("reverse_nested");

        List<SQLExpr> parameters = method.getParameters();

        //
        parameters.removeIf(e -> {
            if (e instanceof SQLBinaryOpExpr && "score_mode".equals(((SQLBinaryOpExpr) e).getLeft().toString())) {
                scoreMode = NestedQueryBuilder.parseScoreMode(Util.expr2Object(((SQLBinaryOpExpr) e).getRight()).toString());
                return true;
            }
            return false;
        });

        int size = parameters.size();
        if (size != 3 && size != 2 && size != 1)
            throw new SqlParseException("on nested object only allowed 3 parameters (path,conditions..,inner_hits) or 2 parameters (field,path)/(path,conditions..) or 1 parameter (field) ");

        // inner_hits
        if (size == 3) {
            this.innerHits = Util.extendedToString(parameters.remove(--size));
        }

        String field = Util.extendedToString(parameters.get(0));
        this.field = field;
        if (size == 1) {
            //calc path myself..
            if (!field.contains(".")) {
                if (!reverse)
                    throw new SqlParseException("nested should contain . on their field name");
                else {
                    this.path = null;
                    this.simple = true;
                }
            } else {
                int lastDot = field.lastIndexOf(".");
                this.path = field.substring(0, lastDot);
                this.simple = true;

            }

        } else if (size == 2) {
            SQLExpr secondParameter = parameters.get(1);
            if(secondParameter instanceof SQLTextLiteralExpr || secondParameter instanceof SQLIdentifierExpr || secondParameter instanceof SQLPropertyExpr) {

                String pathString = Util.extendedToString(secondParameter);
                if(pathString.equals(""))
                    this.path = null;
                else
                    this.path = pathString;
                this.simple = true;
            }
            else {
                this.path = field;
                Where where = Where.newInstance();
                new WhereParser(new SqlParser()).parseWhere(secondParameter,where);
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

    public boolean isReverse() {
        return reverse;
    }

    public String getInnerHits() {
        return innerHits;
    }

    public void setInnerHits(String innerHits) {
        this.innerHits = innerHits;
    }

    public ScoreMode getScoreMode() {
        return scoreMode;
    }
}
