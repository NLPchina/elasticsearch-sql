package org.nlpcn.es4sql.domain;

import java.util.List;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumericLiteralExpr;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.nlpcn.es4sql.Util;
import org.nlpcn.es4sql.exception.SqlParseException;



public class Paramer {
	public String analysis;
	public Float boost;
	public String value;
    public Integer slop;

	public static Paramer parseParamer(SQLMethodInvokeExpr method) throws SqlParseException {
		Paramer instance = new Paramer();
		List<SQLExpr> parameters = method.getParameters();
        for (SQLExpr expr : parameters) {
            if (expr instanceof SQLCharExpr) {
                if (instance.value == null) {
                    instance.value = ((SQLCharExpr) expr).getText();
                } else {
                    instance.analysis = ((SQLCharExpr) expr).getText();
                }
            } else if (expr instanceof SQLNumericLiteralExpr) {
                instance.boost = ((SQLNumericLiteralExpr) expr).getNumber().floatValue();
            } else if (expr instanceof SQLBinaryOpExpr) {
                SQLBinaryOpExpr sqlExpr = (SQLBinaryOpExpr) expr;
                switch (Util.expr2Object(sqlExpr.getLeft()).toString()) {
                    case "query":
                        instance.value = Util.expr2Object(sqlExpr.getRight()).toString();
                        break;
                    case "analyzer":
                        instance.analysis = Util.expr2Object(sqlExpr.getRight()).toString();
                        break;
                    case "boost":
                        instance.boost = Float.parseFloat(Util.expr2Object(sqlExpr.getRight()).toString());
                        break;
                    case "slop":
                        instance.slop = Integer.parseInt(Util.expr2Object(sqlExpr.getRight()).toString());
                        break;
                    default:
                        break;
                }
            }
        }

		return instance;
	}

	public static ToXContent fullParamer(MatchPhraseQueryBuilder query, Paramer paramer) {
		if (paramer.analysis != null) {
			query.analyzer(paramer.analysis);
		}

		if (paramer.boost != null) {
			query.boost(paramer.boost);
		}

        if (paramer.slop != null) {
            query.slop(paramer.slop);
        }

		return query;
	}

	public static ToXContent fullParamer(MatchQueryBuilder query, Paramer paramer) {
		if (paramer.analysis != null) {
			query.analyzer(paramer.analysis);
		}

		if (paramer.boost != null) {
			query.boost(paramer.boost);
		}
		return query;
	}

	public static ToXContent fullParamer(WildcardQueryBuilder query, Paramer paramer) {
		if (paramer.boost != null) {
			query.boost(paramer.boost);
		}
		return query;
	}

    public static ToXContent fullParamer(QueryStringQueryBuilder query, Paramer paramer) {
        if (paramer.analysis != null) {
            query.analyzer(paramer.analysis);
        }

        if (paramer.boost != null) {
            query.boost(paramer.boost);
        }

        if (paramer.slop != null) {
            query.phraseSlop(paramer.slop);
        }

        return query;
    }
}
