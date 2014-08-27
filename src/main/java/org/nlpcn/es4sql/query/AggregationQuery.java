package org.nlpcn.es4sql.query;

import java.util.List;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.nlpcn.es4sql.FilterMaker;
import org.nlpcn.es4sql.domain.Field;
import org.nlpcn.es4sql.domain.KVValue;
import org.nlpcn.es4sql.domain.MethodField;
import org.nlpcn.es4sql.domain.Order;
import org.nlpcn.es4sql.domain.Select;
import org.nlpcn.es4sql.domain.Where;
import org.nlpcn.es4sql.exception.SqlParseException;

public class AggregationQuery extends Query {

	public AggregationQuery(Client client, Select select) {
		super(client, select);
	}

	@Override
	protected SearchRequestBuilder _explan() throws SqlParseException {

		BoolFilterBuilder boolFilter = null;
		// set where
		Where where = select.getWhere();

		TermsBuilder groupByAgg = null;
		FilterAggregationBuilder filter = null;

		if (select.getGroupBys().size() > 0) {
			groupByAgg = AggregationBuilders.terms("group by");
			for (String field : select.getGroupBys()) {
				groupByAgg.field(field);
			}
		}

		if (where != null) {
			boolFilter = FilterMaker.explan(where);
			filter = AggregationBuilders.filter("filter").filter(boolFilter);
			if (groupByAgg != null) {
				filter.subAggregation(groupByAgg);
			}
		}

		// add field
		if (select.getFields().size() > 0) {
			explanFields(request, select.getFields(), groupByAgg, filter);
		}

		request.setSize(0);
		request.setSearchType(SearchType.DEFAULT);

		if (filter != null) {
			request.addAggregation(filter);
		} else if (groupByAgg != null) {
			request.addAggregation(groupByAgg);
		}

		return request;
	}

	public void toResult() {

	}

	private void explanFields(SearchRequestBuilder request, List<Field> fields, TermsBuilder groupByAgg, FilterAggregationBuilder filter) throws SqlParseException {
		for (Field field : fields) {
			if (field instanceof MethodField) {
				AbstractAggregationBuilder makeAgg = makeAgg((MethodField) field);
				if (groupByAgg != null) {
					groupByAgg.subAggregation(makeAgg);
				} else if (filter != null) {
					filter.subAggregation(makeAgg);
				} else {
					request.addAggregation(makeAgg);
				}
			} else if (field instanceof Field) {
				request.addField(field.getName());
			} else {
				throw new SqlParseException("it did not support this field method " + field);
			}
		}
	}

	/**
	 * 将Field 转换为agg
	 * 
	 * @param field
	 * @return
	 * @throws SqlParseException
	 */
	private AbstractAggregationBuilder makeAgg(MethodField field) throws SqlParseException {
		switch (field.getName()) {
		case "SUM":
			return AggregationBuilders.sum(field.getAlias()).field(field.getParams().get(0).toString());
		case "MAX":
			return AggregationBuilders.max(field.getAlias()).field(field.getParams().get(0).toString());
		case "MIN":
			return AggregationBuilders.max(field.getAlias()).field(field.getParams().get(0).toString());
		case "topHits":
			return makeTopHitsAgg(field);

		default:
			throw new SqlParseException("the agg function not to define !");
		}
	}

	private AbstractAggregationBuilder makeTopHitsAgg(MethodField field) {
		TopHitsBuilder topHits = AggregationBuilders.topHits(field.getAlias());
		List<KVValue> params = field.getParams();
		for (KVValue kv : params) {
			switch (kv.key) {
			case "from":
				topHits.setFrom((int) kv.value);
				break;
			case "size":
				topHits.setSize((int) kv.value);
				break;
			default:
				topHits.addSort(kv.key, SortOrder.valueOf(kv.value.toString().toUpperCase()));
				break;
			}
		}
		return topHits;
	}

}
