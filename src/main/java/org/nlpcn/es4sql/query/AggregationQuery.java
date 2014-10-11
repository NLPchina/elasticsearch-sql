package org.nlpcn.es4sql.query;

import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.nlpcn.es4sql.domain.Field;
import org.nlpcn.es4sql.domain.KVValue;
import org.nlpcn.es4sql.domain.MethodField;
import org.nlpcn.es4sql.domain.Order;
import org.nlpcn.es4sql.domain.Select;
import org.nlpcn.es4sql.domain.Where;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.maker.AggMaker;
import org.nlpcn.es4sql.query.maker.FilterMaker;

public class AggregationQuery extends Query {

	private AggMaker aggMaker = new AggMaker();

	public AggregationQuery(Client client, Select select) {
		super(client, select);
	}

	@Override
	protected SearchRequestBuilder _explan() throws SqlParseException {

		BoolFilterBuilder boolFilter = null;
		// set where
		Where where = select.getWhere();

		AggregationBuilder<?> lastAgg = null;
		FilterAggregationBuilder filter = null;

		if (where != null) {
			boolFilter = FilterMaker.explan(where);
			filter = AggregationBuilders.filter("filter").filter(boolFilter);
			request.addAggregation(filter);
		}

		//
		if (select.getGroupBys().size() > 0) {
			Field field = select.getGroupBys().get(0);
			lastAgg = aggMaker.makeGroupAgg(field);
			

			if (lastAgg != null && lastAgg instanceof TermsBuilder) {
				((TermsBuilder) lastAgg).size(select.getRowCount());
			}
			
			if (filter != null) {
				filter.subAggregation(lastAgg);
			} else {
				request.addAggregation(lastAgg);
			}
			for (int i = 1; i < select.getGroupBys().size(); i++) {
				field = select.getGroupBys().get(i);
				AggregationBuilder<?> subAgg = aggMaker.makeGroupAgg(field);
				if(subAgg instanceof TermsBuilder){
					((TermsBuilder)subAgg).size(0) ;
				}
				
				lastAgg.subAggregation(subAgg);
				lastAgg = subAgg;
			}
		}

		Map<String, KVValue> groupMap = aggMaker.getGroupMap();
		// add field
		if (select.getFields().size() > 0) {
			explanFields(request, select.getFields(), lastAgg, filter);
		}

		// add order
		if (lastAgg != null && select.getOrderBys().size() > 0) {
			KVValue temp = null;
			TermsBuilder termsBuilder = null;
			for (Order order : select.getOrderBys()) {
				temp = groupMap.get(order.getName());
				termsBuilder = (TermsBuilder) temp.value;
				switch (temp.key) {
				case "COUNT":
					termsBuilder.order(Terms.Order.count(isASC(order)));
					break;
				case "KEY":
					termsBuilder.order(Terms.Order.term(isASC(order)));
					break;
				case "FIELD":
					termsBuilder.order(Terms.Order.aggregation(order.getName(), isASC(order)));
					break;
				default:
					throw new SqlParseException(order.getName() + " can not to order");
				}
			}
		}
		request.setSize(0);
		request.setSearchType(SearchType.DEFAULT);
		return request;
	}

	private boolean isASC(Order order) {
		return "ASC".equals(order.getType());
	}

	private void explanFields(SearchRequestBuilder request, List<Field> fields, AggregationBuilder<?> groupByAgg, FilterAggregationBuilder filter) throws SqlParseException {
		for (Field field : fields) {
			if (field instanceof MethodField) {
				AbstractAggregationBuilder makeAgg = aggMaker.makeFieldAgg((MethodField) field, groupByAgg);
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
}
