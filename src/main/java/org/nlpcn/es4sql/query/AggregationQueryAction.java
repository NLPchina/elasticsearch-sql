package org.nlpcn.es4sql.query;

import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
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

/**
 * Transform SQL query to Elasticsearch aggregations query
 */
public class AggregationQueryAction extends QueryAction {

	private final Select select;
	private AggMaker aggMaker = new AggMaker();
	private SearchRequestBuilder request;

	public AggregationQueryAction(Client client, Select select) {
		super(client, select);
		this.select = select;
	}

	@Override
	public SearchRequestBuilder explain() throws SqlParseException {
		this.request = client.prepareSearch();
		request.setListenerThreaded(false);
		setIndicesAndTypes();

		setWhere(select.getWhere());
		AggregationBuilder<?> lastAgg = null;

		for (List<Field> groupBy : select.getGroupBys()) {
			if (!groupBy.isEmpty()) {
				Field field = groupBy.get(0);
				lastAgg = aggMaker.makeGroupAgg(field);

				if (lastAgg != null && lastAgg instanceof TermsBuilder) {
					((TermsBuilder) lastAgg).size(select.getRowCount());
				}

				request.addAggregation(lastAgg);

				for (int i = 1; i < groupBy.size(); i++) {
					field = groupBy.get(i);
					AggregationBuilder<?> subAgg = aggMaker.makeGroupAgg(field);
					if (subAgg instanceof TermsBuilder) {
						((TermsBuilder) subAgg).size(0);
					}

					lastAgg.subAggregation(subAgg);
					lastAgg = subAgg;
				}
			}
		}

		Map<String, KVValue> groupMap = aggMaker.getGroupMap();
		// add field
		if (select.getFields().size() > 0) {
			explanFields(request, select.getFields(), lastAgg);
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

	private void explanFields(SearchRequestBuilder request, List<Field> fields, AggregationBuilder<?> groupByAgg) throws SqlParseException {
		for (Field field : fields) {
			if (field instanceof MethodField) {
				AbstractAggregationBuilder makeAgg = aggMaker.makeFieldAgg((MethodField) field, groupByAgg);
				if (groupByAgg != null) {
					groupByAgg.subAggregation(makeAgg);
				}
				 else {
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
	 * Create filters based on
	 * the Where clause.
	 * @param where the 'WHERE' part of the SQL query.
	 * @throws SqlParseException
	 */
	private void setWhere(Where where) throws SqlParseException {
		if (where != null) {
			BoolFilterBuilder boolFilter = FilterMaker.explan(where);
			request.setQuery(QueryBuilders.filteredQuery(null, boolFilter));
		}
	}


	/**
	 * Set indices and types to the search request.
	 */
	private void setIndicesAndTypes() {
		request.setIndices(query.getIndexArr());

		String[] typeArr = query.getTypeArr();
		if(typeArr != null) {
			request.setTypes(typeArr);
		}
	}
}
