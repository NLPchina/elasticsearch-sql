//package org.nlpcn.es4sql.query.maker;
//
//import org.elasticsearch.index.query.BaseFilterBuilder;
//import org.elasticsearch.index.query.BoolFilterBuilder;
//import org.elasticsearch.index.query.FilterBuilders;
//import org.nlpcn.es4sql.domain.Condition;
//import org.nlpcn.es4sql.domain.Where;
//import org.nlpcn.es4sql.domain.Where.CONN;
//import org.nlpcn.es4sql.exception.SqlParseException;
//
//public class FilterMaker extends Maker {
//
//	/**
//	 * 将where条件构建成filter
//	 *
//	 * @param where
//	 * @return
//	 * @throws SqlParseException
//	 */
//	public static BoolFilterBuilder explan(Where where) throws SqlParseException {
//		BoolFilterBuilder boolFilter = FilterBuilders.boolFilter();
//		while (where.getWheres().size() == 1) {
//			where = where.getWheres().getFirst();
//		}
//		new FilterMaker().explanWhere(boolFilter, where);
//		return boolFilter;
//	}
//
//	private FilterMaker() {
//		super(false);
//	}
//
//	private void explanWhere(BoolFilterBuilder boolFilter, Where where) throws SqlParseException {
//		if (where instanceof Condition) {
//			addSubFilter(boolFilter, where, (BaseFilterBuilder) make((Condition) where));
//		} else {
//			BoolFilterBuilder subFilter = FilterBuilders.boolFilter();
//			addSubFilter(boolFilter, where, subFilter);
//			for (Where subWhere : where.getWheres()) {
//				explanWhere(subFilter, subWhere);
//			}
//		}
//	}
//
//	/**
//	 * 增加嵌套插
//	 *
//	 * @param boolFilter
//	 * @param where
//	 * @param subFilter
//	 */
//	private void addSubFilter(BoolFilterBuilder boolFilter, Where where, BaseFilterBuilder subFilter) {
//		if (where.getConn() == CONN.AND) {
//			boolFilter.must(subFilter);
//		} else {
//			boolFilter.should(subFilter);
//		}
//	}
//
//}
