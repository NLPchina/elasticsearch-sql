package org.nlpcn.es4sql.parse;

import java.util.ArrayList;
import java.util.List;

import org.nlpcn.es4sql.Util;
import org.nlpcn.es4sql.domain.Condition;
import org.nlpcn.es4sql.domain.Condition.OPEAR;
import org.nlpcn.es4sql.domain.Where;
import org.nlpcn.es4sql.exception.SqlParseException;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCaseExpr;
import com.google.common.base.Joiner;

/**
 * Created by allwefantasy on 9/3/16.
 */
public class CaseWhenParser {
    private SQLCaseExpr caseExpr;
    private String alias;
    private String tableAlias;

    private final static String OPERATOR_AND = " && ";
    private final static String OPERATOR_OR = " || ";

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
            if (scriptCode.startsWith(OPERATOR_AND) || scriptCode.startsWith(OPERATOR_OR)) {
                scriptCode = scriptCode.substring(4);
            }
            if (result.size() == 0) {
                result.add(
                        "if(" + scriptCode + ")" + "{" + Util.getScriptValueWithQuote(item.getValueExpr(), "'") + "}");
            } else {
                result.add("else if(" + scriptCode + ")" + "{" + Util.getScriptValueWithQuote(item.getValueExpr(), "'")
                        + "}");
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
        String relation = where.getConn().name().equals("AND") ? OPERATOR_AND : OPERATOR_OR;
        String express = Joiner.on("").join(codes);
        if (express.startsWith(OPERATOR_AND) || express.startsWith(OPERATOR_OR)) {
            express = express.substring(4);
        }
        return relation + express;
    }

    private void explainWhere(List<String> codes, Where where) throws SqlParseException {
        if (where instanceof Condition) {
            Condition condition = (Condition) where;
            String relation = condition.getConn().name().equals("AND") ? OPERATOR_AND : OPERATOR_OR;
            if (condition.getValue() instanceof ScriptFilter) {
                codes.add(relation + ((ScriptFilter) condition.getValue()).getScript());
            } else if (condition.getOpear() == OPEAR.BETWEEN) {
                Object[] objs = (Object[]) condition.getValue();
                codes.add(relation + "doc['" + condition.getName() + "'].value >= " + objs[0] + " && doc['"
                        + condition.getName() + "'].value <=" + objs[1]);
            } else {
                codes.add(relation + Util.getScriptValueWithQuote(condition.getNameExpr(), "'")
                        + condition.getOpertatorSymbol() + Util.getScriptValueWithQuote(condition.getValueExpr(), "'"));
            }
        } else {
            for (Where subWhere : where.getWheres()) {
                List<String> subCodes = new ArrayList<String>();
                explainWhere(subCodes, subWhere);
                String relation = subWhere.getConn().name().equals("AND") ? OPERATOR_AND : OPERATOR_OR;
                if (subCodes.size() > 1) {
                    String express = Joiner.on("").join(subCodes);
                    if (express.startsWith(OPERATOR_AND) || express.startsWith(OPERATOR_OR)) {
                        express = express.substring(4);
                    }
                    codes.add(relation + "(" + express + ")");
                } else {
                    codes.add(Joiner.on(relation).join(subCodes));
                }
            }
        }
    }
}
