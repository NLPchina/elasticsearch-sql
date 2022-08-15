package org.nlpcn.es4sql.query.maker;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.join.query.JoinQueryBuilders;
import org.nlpcn.es4sql.Util;
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

        //zhongshu-comment 一直取，取到最深的那个where
		//zhongshu-comment 暂时只遇到了该sql：select a,b,c as my_c from tbl where a = 1，会走这个分支
		//就是where子句中只有一个条件的情况下会走该分支
		//zhongshu-comment 那他为什么用while呢？？用if不就得吗？那应该是还有层层嵌套的情况，一直get到底
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

		//zhongshu-comment 暂时只遇到了该sql：select a,b,c as my_c from tbl where a = 1，会走这个分支
		if (where instanceof Condition) {
			addSubQuery(
					boolQuery,
					where,
					(QueryBuilder) make((Condition) where) //zhongshu-comment 重点方法 就是这里解析最细粒度的where条件
			);
		} else {
			/*
			zhongshu-comment select a,b,c as my_c from tbl where a = 1 or b = 2 and (c = 3 or d = 4) or e > 1
			上面这条sql中的“b = 2 and (c = 3 or d = 4)”这部分会走该分支，
			因为“b = 2 and (c = 3 or d = 4)”被封装为Where类型的对象，而不是Condition对象
			对应的具体笔记见：搜索-->es插件开发-->es-sql-->代码阅读-->如何解析where条件
			 */
			BoolQueryBuilder subQuery = QueryBuilders.boolQuery();

			//zhongshu-comment 将subQuery对象纳入到boolQuery中，即boolQuery是上一级，subQuery是下一级
			addSubQuery(boolQuery, where, subQuery);
			for (Where subWhere : where.getWheres()) {
				//zhongshu-comment 然后又将subWhere对象纳入到subQuery对象中，通过递归就能层层解析出这个Where条件了：“b = 2 and (c = 3 or d = 4)”
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
				boolean isNestedQuery = subQuery instanceof NestedQueryBuilder;
				InnerHitBuilder ihb = null;
				if (condition.getInnerHits() != null) {
                    try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, condition.getInnerHits())) {
                        ihb = InnerHitBuilder.fromXContent(parser);
                    } catch (IOException e) {
                        throw new IllegalArgumentException("couldn't parse inner_hits: " + e.getMessage(), e);
                    }
                }

                // bugfix #628
                if ("missing".equalsIgnoreCase(String.valueOf(condition.getValue())) && (condition.getOpear() == Condition.OPEAR.IS || condition.getOpear() == Condition.OPEAR.EQ)) {
                    NestedQueryBuilder q = isNestedQuery ? (NestedQueryBuilder) subQuery : QueryBuilders.nestedQuery(condition.getNestedPath(), QueryBuilders.boolQuery().mustNot(subQuery), condition.getScoreMode());
					if (ihb != null) {
						q.innerHit(ihb);
                    }
                    boolQuery.mustNot(q);
                    return;
                }

                // support not nested
                if (condition.getOpear() == Condition.OPEAR.NNESTED_COMPLEX) {
                    if (ihb != null) {
                        NestedQueryBuilder.class.cast(subQuery).innerHit(ihb);
                    }
                    boolQuery.mustNot(subQuery);
                    return;
                }

                if (!isNestedQuery) {
					subQuery = QueryBuilders.nestedQuery(condition.getNestedPath(), subQuery, condition.getScoreMode());
				}
                if (ihb != null) {
                    ((NestedQueryBuilder) subQuery).innerHit(ihb);
                }
            } else if(condition.isChildren()) {
            	subQuery = Util.parseQueryBuilder(JoinQueryBuilders.hasChildQuery(condition.getChildType(), subQuery, ScoreMode.None));
            }
        }

		//zhongshu-comment 将subQuery对象纳入到boolQuery中，即boolQuery是上一级，subQuery是下一级
		if (where.getConn() == CONN.AND) {
			boolQuery.must(subQuery);
		} else {
			boolQuery.should(subQuery);
		}
	}
}
