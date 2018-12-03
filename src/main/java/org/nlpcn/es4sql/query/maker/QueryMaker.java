package org.nlpcn.es4sql.query.maker;

import com.fasterxml.jackson.core.JsonFactory;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.json.JsonXContentParser;
import org.elasticsearch.index.query.*;
import org.elasticsearch.join.query.JoinQueryBuilders;
import org.nlpcn.es4sql.domain.Condition;
import org.nlpcn.es4sql.domain.Where;
import org.nlpcn.es4sql.domain.Where.CONN;
import org.nlpcn.es4sql.exception.SqlParseException;

import java.io.IOException;

public class QueryMaker extends Maker {

	/**
	 * 将where条件构建成query
	 * 
	 * @param where
	 * @return
	 * @throws SqlParseException
	 */
	public static BoolQueryBuilder explan(Where where) throws SqlParseException {
		return explan(where,true);
	}

    public static BoolQueryBuilder explan(Where where,boolean isQuery) throws SqlParseException {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        while (where.getWheres().size() == 1) {
            where = where.getWheres().getFirst();
        }
        new QueryMaker().explanWhere(boolQuery, where);
        if(isQuery){
            return boolQuery;
        }
        return QueryBuilders.boolQuery().filter(boolQuery);
    }

	private QueryMaker() {
		super(true);
	}

	private void explanWhere(BoolQueryBuilder boolQuery, Where where) throws SqlParseException {
		if (where instanceof Condition) {
			addSubQuery(boolQuery, where, (QueryBuilder) make((Condition) where));
		} else {
			BoolQueryBuilder subQuery = QueryBuilders.boolQuery();
			addSubQuery(boolQuery, where, subQuery);
			for (Where subWhere : where.getWheres()) {
				explanWhere(subQuery, subWhere);
			}
		}
	}

	/**
	 * 增加嵌套插
	 * 
	 * @param boolQuery
	 * @param where
	 * @param subQuery
	 */
	private void addSubQuery(BoolQueryBuilder boolQuery, Where where, QueryBuilder subQuery) {
        if(where instanceof Condition){
            Condition condition = (Condition) where;

            if (condition.isNested() && !(subQuery instanceof NestedQueryBuilder)) {
                InnerHitBuilder ihb = null;
                if (condition.getInnerHits() != null) {
                    try (JsonXContentParser parser = new JsonXContentParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, new JsonFactory().createParser(condition.getInnerHits()))) {
                        ihb = InnerHitBuilder.fromXContent(parser);
                    } catch (IOException e) {
                        throw new IllegalArgumentException("couldn't parse inner_hits: " + e.getMessage(), e);
                    }
                }

                // bugfix #628
                if ("missing".equalsIgnoreCase(String.valueOf(condition.getValue())) && (condition.getOpear() == Condition.OPEAR.IS || condition.getOpear() == Condition.OPEAR.EQ)) {
                    NestedQueryBuilder q = QueryBuilders.nestedQuery(condition.getNestedPath(), QueryBuilders.boolQuery().mustNot(subQuery), ScoreMode.None);
                    if (ihb != null) {
                        q.innerHit(ihb);
                    }
                    boolQuery.mustNot(q);
                    return;
                }

                subQuery = QueryBuilders.nestedQuery(condition.getNestedPath(), subQuery, ScoreMode.None);
                if (ihb != null) {
                    ((NestedQueryBuilder) subQuery).innerHit(ihb);
                }
            } else if(condition.isChildren()) {
            	subQuery = JoinQueryBuilders.hasChildQuery(condition.getChildType(), subQuery, ScoreMode.None);
            }
        }

		if (where.getConn() == CONN.AND) {
			boolQuery.must(subQuery);
		} else {
			boolQuery.should(subQuery);
		}
	}
}
