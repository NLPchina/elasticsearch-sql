package org.nlpcn.es4sql.parse;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCaseExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.google.common.base.Joiner;
import org.elasticsearch.common.inject.internal.Join;
import org.nlpcn.es4sql.SQLFunctions;
import org.nlpcn.es4sql.Util;
import org.nlpcn.es4sql.domain.Condition;
import org.nlpcn.es4sql.domain.Condition.OPEAR;
import org.nlpcn.es4sql.domain.Where;
import org.nlpcn.es4sql.exception.SqlParseException;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by allwefantasy on 9/3/16.
 */
public class CaseWhenParser {
    private SQLCaseExpr caseExpr;
    private String alias;
    private String tableAlias;

    public CaseWhenParser(SQLCaseExpr caseExpr, String alias, String tableAlias) {
        this.alias = alias;
        this.tableAlias = tableAlias;
        this.caseExpr = caseExpr;

    }

    public String parse() throws SqlParseException {
        List<String> result = new ArrayList<String>();

        for (SQLCaseExpr.Item item : caseExpr.getItems()) {
            SQLExpr conditionExpr = item.getConditionExpr();

            WhereParser parser = new WhereParser(new SqlParser(), conditionExpr);
            String scriptCode = explain(parser.findWhere());
            if (scriptCode.startsWith(" &&")) {
                scriptCode = scriptCode.substring(3);
            }
            if (result.size() == 0) {
                result.add("if(" + scriptCode + ")" + "{" + Util.getScriptValueWithQuote(item.getValueExpr(), "'") + "}");
            } else {
                result.add("else if(" + scriptCode + ")" + "{" + Util.getScriptValueWithQuote(item.getValueExpr(), "'") + "}");
            }

        }
        SQLExpr elseExpr = caseExpr.getElseExpr();
        if (elseExpr == null) {
            result.add("else { null }");
        } else {
            result.add("else {" + Util.getScriptValueWithQuote(elseExpr, "'") + "}");
        }


        return Joiner.on(" ").join(result);
    }

    public String explain(Where where) throws SqlParseException {
        List<String> codes = new ArrayList<String>();
        while (where.getWheres().size() == 1) {
            where = where.getWheres().getFirst();
        }
        explainWhere(codes, where);
        String relation = where.getConn().name().equals("AND") ? " && " : " || ";
        return Joiner.on(relation).join(codes);
    }


    private void explainWhere(List<String> codes, Where where) throws SqlParseException {
        if (where instanceof Condition) {
            Condition condition = (Condition) where;

            if (condition.getValue() instanceof ScriptFilter) {
                codes.add("(" + ((ScriptFilter) condition.getValue()).getScript() + ")");
            } else if (condition.getOpear() == OPEAR.BETWEEN) {
                Object[] objs = (Object[]) condition.getValue();
                codes.add("(" + "doc['" + condition.getName() + "'].value >= " + objs[0] + " && doc['"
                        + condition.getName() + "'].value <=" + objs[1] + ")");
            } else {
                SQLExpr nameExpr = condition.getNameExpr();
                SQLExpr valueExpr = condition.getValueExpr();
                if(valueExpr instanceof SQLNullExpr) {
                    codes.add("(" + "doc['" + nameExpr.toString() + "']" + ".empty)");
                } else {//zhongshu-comment (doc['c'].value=='1')
                    codes.add("(" + Util.getScriptValueWithQuote(nameExpr, "'") + condition.getOpertatorSymbol() + Util.getScriptValueWithQuote(valueExpr, "'") + ")");
                }
            }
        } else {
            for (Where subWhere : where.getWheres()) {
                List<String> subCodes = new ArrayList<String>();
                explainWhere(subCodes, subWhere);
                String relation = subWhere.getConn().name().equals("AND") ? "&&" : "||";
                codes.add(Joiner.on(relation).join(subCodes));
            }
        }
    }

}
