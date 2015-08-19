package org.nlpcn.es4sql.query;

import java.util.List;
import java.util.ArrayList;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.nlpcn.es4sql.domain.Field;
import org.nlpcn.es4sql.domain.Order;
import org.nlpcn.es4sql.domain.Select;
import org.nlpcn.es4sql.domain.Where;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.maker.FilterMaker;
import org.nlpcn.es4sql.query.maker.QueryMaker;

/**
 * Transform SQL query to standard Elasticsearch search query
 */
public class DefaultQueryAction extends QueryAction {

	private final Select select;
	private SearchRequestBuilder request;

	public DefaultQueryAction(Client client, Select select) {
		super(client, select);
		this.select = select;
	}

	@Override
	public SqlElasticSearchRequestBuilder explain() throws SqlParseException {
		this.request = client.prepareSearch();
		request.setListenerThreaded(false);
		setIndicesAndTypes();

		setFields(select.getFields());
		setWhere(select.getWhere());
		setSorts(select.getOrderBys());
		setLimit(select.getOffset(), select.getRowCount());

		// set SearchType.
		request.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
        SqlElasticSearchRequestBuilder sqlElasticRequestBuilder = new SqlElasticSearchRequestBuilder(request);
		return sqlElasticRequestBuilder;
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


	/**
	 * Set source filtering on a search request.
	 * @param fields list of fields to source filter.
	 */
	private void setFields(List<Field> fields) {
		if (select.getFields().size() > 0) {
			ArrayList<String> includeFields = new ArrayList<String>();

			for (Field field : fields) {
				if (field instanceof Field) {
					includeFields.add(field.getName());
				}
			}

			request.setFetchSource(includeFields.toArray(new String[includeFields.size()]), null);
		}
	}


	/**
	 * Create filters or queries based on
	 * the Where clause.
	 * @param where the 'WHERE' part of the SQL query.
	 * @throws SqlParseException
	 */
	private void setWhere(Where where) throws SqlParseException {
		if (where != null) {
			if (select.isQuery) {
				BoolQueryBuilder boolQuery = QueryMaker.explan(where);
				request.setQuery(boolQuery);
			} else {
				BoolFilterBuilder boolFilter = FilterMaker.explan(where);
				request.setQuery(QueryBuilders.filteredQuery(null, boolFilter));
			}
		}
	}


	/**
	 * Add sorts to the elasticsearch query
	 * based on the 'ORDER BY' clause.
	 * @param orderBys list of Order object
	 */
	private void setSorts(List<Order> orderBys) {
		for (Order order : orderBys) {
			request.addSort(order.getName(), SortOrder.valueOf(order.getType()));
		}
	}


	/**
	 * Add from and size to the ES query
	 * based on the 'LIMIT' clause
	 * @param from starts from document at position from
	 * @param size number of documents to return.
	 */
	private void setLimit(int from, int size) {
		request.setFrom(from);

		if (size > -1) {
			request.setSize(size);
		}
	}


}
