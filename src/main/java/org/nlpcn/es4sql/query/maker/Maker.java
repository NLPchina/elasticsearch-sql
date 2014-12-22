package org.nlpcn.es4sql.query.maker;

import java.util.Set;

import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.index.query.*;
import org.nlpcn.es4sql.domain.Condition;
import org.nlpcn.es4sql.domain.Condition.OPEAR;
import org.nlpcn.es4sql.domain.Paramer;
import org.nlpcn.es4sql.exception.SqlParseException;

import org.durid.sql.ast.expr.SQLIdentifierExpr;
import org.durid.sql.ast.expr.SQLMethodInvokeExpr;

public abstract class Maker {

	private static final Set<OPEAR> NOT_OPEAR_SET = Sets.newHashSet(OPEAR.N, OPEAR.NIN, OPEAR.ISN, OPEAR.NBETWEEN);

	private boolean isQuery = false;

	protected Maker(Boolean isQuery) {
		this.isQuery = isQuery;
	}

	/**
	 * 构建过滤条件
	 * 
	 * @param boolFilter
	 * @param expr
	 * @param expr
	 * @return
	 * @throws SqlParseException
	 */
	protected ToXContent make(Condition cond) throws SqlParseException {

		String name = cond.getName();
		Object value = cond.getValue();

		ToXContent x = null;
		if (value instanceof SQLMethodInvokeExpr) {
			x = make(cond, name, (SQLMethodInvokeExpr) value);
		} else {
			x = make(cond, name, value);
		}

		return x;
	}

	private ToXContent make(Condition cond, String name, SQLMethodInvokeExpr value) throws SqlParseException {
		ToXContent bqb = null;
		Paramer paramer = null;
		switch (value.getMethodName().toLowerCase()) {
		case "query":
			paramer = Paramer.parseParamer(value);
			QueryStringQueryBuilder queryString = QueryBuilders.queryString(paramer.value);
			bqb = Paramer.fullParamer(queryString, paramer);
			if (!isQuery) {
				bqb = FilterBuilders.queryFilter((QueryBuilder) bqb);
			}
			bqb = fixNot(cond, bqb);
			break;
		case "matchquery":
		case "match_query":
			paramer = Paramer.parseParamer(value);
			MatchQueryBuilder matchQuery = QueryBuilders.matchQuery(name, paramer.value);
			bqb = Paramer.fullParamer(matchQuery, paramer);
			if (!isQuery) {
				bqb = FilterBuilders.queryFilter((QueryBuilder) bqb);
			}
			bqb = fixNot(cond, bqb);
			break;
		case "score":
		case "scorequery":
		case "score_query":
			Float boost = Float.parseFloat(value.getParameters().get(1).toString());
			Condition subCond = new Condition(cond.getConn(), cond.getName(), cond.getOpear(), value.getParameters().get(0));
			if (isQuery) {
				bqb = QueryBuilders.constantScoreQuery((QueryBuilder) make(subCond)).boost(boost);
			} else {
				bqb = QueryBuilders.constantScoreQuery((FilterBuilder) make(subCond)).boost(boost);
				bqb = FilterBuilders.queryFilter((QueryBuilder) bqb);
			}
			break;
		case "wildcardquery":
		case "wildcard_query":
			paramer = Paramer.parseParamer(value);
			WildcardQueryBuilder wildcardQuery = QueryBuilders.wildcardQuery(name, paramer.value);
			bqb = Paramer.fullParamer(wildcardQuery, paramer);
			if (!isQuery) {
				bqb = FilterBuilders.queryFilter((QueryBuilder) bqb);
			}
			break;

		case "matchphrasequery":
		case "match_phrase":
		case "matchphrase":
			paramer = Paramer.parseParamer(value);
			MatchQueryBuilder matchPhraseQuery = QueryBuilders.matchPhraseQuery(name, paramer.value);
			bqb = Paramer.fullParamer(matchPhraseQuery, paramer);
			if (!isQuery) {
				bqb = FilterBuilders.queryFilter((QueryBuilder) bqb);
			}
			break;
		default:
			throw new SqlParseException("it did not support this query method " + value.getMethodName());

		}

		return bqb;
	}

	private ToXContent make(Condition cond, String name, Object value) throws SqlParseException {
		ToXContent x = null;
		switch (cond.getOpear()) {
		case ISN:
		case IS:
		case N:
		case EQ:
			if (value instanceof SQLIdentifierExpr) {
				SQLIdentifierExpr identifier = (SQLIdentifierExpr) value;
				if(identifier.getName().equalsIgnoreCase("missing")) {
					x = FilterBuilders.missingFilter(name);
					if (isQuery) {
						x = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), FilterBuilders.missingFilter(name));
					}
				}
				else {
					throw new SqlParseException(String.format("Cannot recoginze Sql identifer %s", identifier.getName()));
				}
				break;
			} else {
				// TODO, maybe use term filter when not analayzed field avalaible to make exact matching?
				// using matchPhrase to achieve equallity.
				// matchPhrase still have some disatvantegs, f.e search for 'word' will match 'some word'
				MatchQueryBuilder matchPhraseQuery = QueryBuilders.matchPhraseQuery(name, value);
				x = isQuery? matchPhraseQuery : FilterBuilders.queryFilter(matchPhraseQuery);
				break;
			}
		case LIKE:
			String queryStr = ((String) value).replace('%', '*').replace('_', '?');
			WildcardQueryBuilder wildcardQuery = QueryBuilders.wildcardQuery(name, queryStr);
			x = isQuery ? wildcardQuery : FilterBuilders.queryFilter(wildcardQuery);
			break;
		case GT:
			if (isQuery)
				x = QueryBuilders.rangeQuery(name).gt(value);
			else
				x = FilterBuilders.rangeFilter(name).gt(value);

			break;
		case GTE:
			if (isQuery)
				x = QueryBuilders.rangeQuery(name).gte(value);
			else
				x = FilterBuilders.rangeFilter(name).gte(value);
			break;
		case LT:
			if (isQuery)
				x = QueryBuilders.rangeQuery(name).lt(value);
			else
				x = FilterBuilders.rangeFilter(name).lt(value);

			break;
		case LTE:
			if (isQuery)
				x = QueryBuilders.rangeQuery(name).lte(value);
			else
				x = FilterBuilders.rangeFilter(name).lte(value);
			break;
		case NIN:
		case IN:
			Object[] values = (Object[]) value;
			MatchQueryBuilder[] matchQueries = new MatchQueryBuilder[values.length];
			for(int i = 0; i < values.length; i++) {
				matchQueries[i] = QueryBuilders.matchPhraseQuery(name, values[i]);
			}

			if(isQuery) {
				BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
				for(MatchQueryBuilder matchQuery : matchQueries) {
					boolQuery.should(matchQuery);
				}
				x = boolQuery;
			}
			else {
				OrFilterBuilder orFilter = FilterBuilders.orFilter();
				for(MatchQueryBuilder matchQuery : matchQueries) {
					orFilter.add(FilterBuilders.queryFilter(matchQuery));
				}
				x = orFilter;
			}
			break;
		case BETWEEN:
		case NBETWEEN:
			if (isQuery)
				x = QueryBuilders.rangeQuery(name).gte(((Object[]) value)[0]).lte(((Object[]) value)[1]);
			else
				x = FilterBuilders.rangeFilter(name).gte(((Object[]) value)[0]).lte(((Object[]) value)[1]);
			break;
		default:
			throw new SqlParseException("not define type " + cond.getName());
		}

		x = fixNot(cond, x);
		return x;
	}

	private ToXContent fixNot(Condition cond, ToXContent bqb) {
		if (NOT_OPEAR_SET.contains(cond.getOpear())) {
			if (isQuery) {
				bqb = QueryBuilders.boolQuery().mustNot((QueryBuilder) bqb);
			} else {
				bqb = FilterBuilders.notFilter((FilterBuilder) bqb);
			}
		}
		return bqb;
	}

}
