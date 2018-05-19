package org.nlpcn.es4sql.query.maker;


import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.join.query.JoinQueryBuilders;
import org.nlpcn.es4sql.domain.Condition;
import org.nlpcn.es4sql.domain.Where;
import org.nlpcn.es4sql.domain.Where.CONN;
import org.nlpcn.es4sql.exception.SqlParseException;

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

        //zhongshu-comment 一直取，取到最深的那个where
        while (where.getWheres().size() == 1) {
            where = where.getWheres().getFirst();
        }

        //zhongshu-comment where.getWheres().size()的长度等于0 或者 大于1
        new QueryMaker().explanWhere(boolQuery, where);
		//zhongshu-comment isQuery为true，应该就是要计算_score的
        if(isQuery){
            return boolQuery;
        }
        //zhongshu-comment isQuery为false，应该就是使用filter，不需要计算_score
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

            if (condition.isNested()) {
                // bugfix #628
                if ("missing".equalsIgnoreCase(String.valueOf(condition.getValue())) && (condition.getOpear() == Condition.OPEAR.IS || condition.getOpear() == Condition.OPEAR.EQ)) {
                    boolQuery.mustNot(QueryBuilders.nestedQuery(condition.getNestedPath(), QueryBuilders.boolQuery().mustNot(subQuery), ScoreMode.None));
                    return;
                }

                subQuery = QueryBuilders.nestedQuery(condition.getNestedPath(), subQuery, ScoreMode.None);
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
