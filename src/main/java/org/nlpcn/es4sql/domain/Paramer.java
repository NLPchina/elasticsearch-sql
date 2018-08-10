package org.nlpcn.es4sql.domain;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumericLiteralExpr;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.index.query.*;
import org.nlpcn.es4sql.Util;
import org.nlpcn.es4sql.exception.SqlParseException;



public class Paramer {
	public String analysis;
	public Float boost;
	public String value;
    public Integer slop;

    public Map<String, Float> fieldsBoosts = new HashMap<>();
    public String type;
    public Float tieBreaker;
    public Operator operator;

    public String default_field;

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

                    case "fields":
                        int index;
                        for (String f : Strings.splitStringByCommaToArray(Util.expr2Object(sqlExpr.getRight()).toString())) {
                            index = f.lastIndexOf('^');
                            if (-1 < index) {
                                instance.fieldsBoosts.put(f.substring(0, index), Float.parseFloat(f.substring(index + 1)));
                            } else {
                                instance.fieldsBoosts.put(f, 1.0F);
                            }
                        }
                        break;
                    case "type":
                        instance.type = Util.expr2Object(sqlExpr.getRight()).toString();
                        break;
                    case "tie_breaker":
                        instance.tieBreaker = Float.parseFloat(Util.expr2Object(sqlExpr.getRight()).toString());
                        break;
                    case "operator":
                        instance.operator = Operator.fromString(Util.expr2Object(sqlExpr.getRight()).toString());
                        break;

                    case "default_field":
                        instance.default_field = Util.expr2Object(sqlExpr.getRight()).toString();
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

        if (paramer.default_field != null) {
            query.defaultField(paramer.default_field);
        }

        return query;
    }

    public static ToXContent fullParamer(MultiMatchQueryBuilder query, Paramer paramer) {
        if (paramer.analysis != null) {
            query.analyzer(paramer.analysis);
        }

        if (paramer.boost != null) {
            query.boost(paramer.boost);
        }

        if (paramer.slop != null) {
            query.slop(paramer.slop);
        }

        if (paramer.type != null) {
            query.type(paramer.type);
        }

        if (paramer.tieBreaker != null) {
            query.tieBreaker(paramer.tieBreaker);
        }

        if (paramer.operator != null) {
            query.operator(paramer.operator);
        }

        return query;
    }
}
