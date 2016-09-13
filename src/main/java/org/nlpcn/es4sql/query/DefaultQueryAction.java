package org.nlpcn.es4sql.query;

import java.util.List;
import java.util.ArrayList;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.nlpcn.es4sql.domain.*;
import org.nlpcn.es4sql.domain.hints.Hint;
import org.nlpcn.es4sql.domain.hints.HintType;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.maker.FilterMaker;
import org.nlpcn.es4sql.query.maker.QueryMaker;

/**
 * Transform SQL query to standard Elasticsearch search query
 */
public class
        DefaultQueryAction extends QueryAction {

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

        boolean usedScroll = useScrollIfNeeded(select.isOrderdSelect());
        if(!usedScroll){
            request.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
        }
        updateRequestWithIndexAndRoutingOptions(select, request);
        updateRequestWithHighlight(select, request);

        SqlElasticSearchRequestBuilder sqlElasticRequestBuilder = new SqlElasticSearchRequestBuilder(request);

		return sqlElasticRequestBuilder;
	}

    private boolean useScrollIfNeeded(boolean existsOrderBy) {
        Hint scrollHint = null;
        for(Hint hint: select.getHints()){
            if(hint.getType() == HintType.USE_SCROLL){
                scrollHint = hint;
                break;
            }
        }
        if(scrollHint!=null) {
            int scrollSize = (Integer) scrollHint.getParams()[0];
            int timeoutInMilli = (Integer) scrollHint.getParams()[1];
            if(!existsOrderBy) request.setSearchType(SearchType.SCAN);
            request.setScroll(new TimeValue(timeoutInMilli))
                    .setSize(scrollSize);
        }
        return scrollHint !=null ;
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
	private void setFields(List<Field> fields) throws SqlParseException {
		if (select.getFields().size() > 0) {
			ArrayList<String> includeFields = new ArrayList<String>();

			for (Field field : fields) {
                if(field instanceof MethodField){
                    handleMethodField((MethodField) field);
                }
				else if (field instanceof Field) {
					includeFields.add(field.getName());
				}
			}

			request.setFetchSource(includeFields.toArray(new String[includeFields.size()]), null);
		}
	}

    private void handleMethodField(MethodField field) throws SqlParseException {
        MethodField method = (MethodField) field;
        if(method.getName().toLowerCase().equals("script")){
            handleScriptField(method);
        }
    }

    private void handleScriptField(MethodField method) throws SqlParseException {
        List<KVValue> params = method.getParams();
        if(params.size() == 2){
            request.addScriptField(params.get(0).value.toString(),params.get(1).value.toString());
        }
        else if(params.size() == 3){
            request.addScriptField(params.get(0).value.toString(),params.get(1).value.toString(),params.get(2).value.toString(),null);
        }
        else {
            throw new SqlParseException("scripted_field only allows script(name,script) or script(name,lang,script)");
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

    public SearchRequestBuilder getRequestBuilder() {
        return request;
    }
}
