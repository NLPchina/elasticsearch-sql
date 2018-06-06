package org.nlpcn.es4sql.query;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

import org.apache.lucene.search.Sort;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.sort.*;
import org.nlpcn.es4sql.domain.*;
import org.nlpcn.es4sql.domain.hints.Hint;
import org.nlpcn.es4sql.domain.hints.HintType;
import org.nlpcn.es4sql.exception.SqlParseException;
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

	/**
	 * zhongshu-comment 只被调用了一次，就在AggregationQueryAction类中
	 * @param request
	 * @throws SqlParseException
	 */
	public void intialize(SearchRequestBuilder request) throws SqlParseException {
		this.request = request;
	}

	//zhongshu-comment 将sql字符串解析后的java对象，转换为es的查询请求对象
	@Override
	public SqlElasticSearchRequestBuilder explain() throws SqlParseException {
		this.request = client.prepareSearch();//zhongshu-comment conflict
		setIndicesAndTypes();

		//zhongshu-comment 将Select对象中封装的sql token信息转换并传到成员变量es搜索请求对象request中
		setFields(select.getFields());

		setWhere(select.getWhere());
		setSorts(select.getOrderBys()); //zhongshu-comment
		setLimit(select.getOffset(), select.getRowCount());

		boolean usedScroll = useScrollIfNeeded(select.isOrderdSelect());
		if (!usedScroll) {
			request.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
		}

		//zhongshu-comment The routing values to control the shards that the search will be executed on.
		updateRequestWithIndexAndRoutingOptions(select, request);
		updateRequestWithHighlight(select, request);
		updateRequestWithCollapse(select, request);

		//zhongshu-comment 后置过滤，对查询结果做后置过滤，这个过滤仅仅作用于返回的hits数据，不会影响到aggs的结果，aggs的结果本来是怎样的就是怎样的
		updateRequestWithPostFilter(select, request);
		SqlElasticSearchRequestBuilder sqlElasticRequestBuilder = new SqlElasticSearchRequestBuilder(request);

		return sqlElasticRequestBuilder;
	}

	private boolean useScrollIfNeeded(boolean existsOrderBy) {
		Hint scrollHint = null;
		for (Hint hint : select.getHints()) {
			if (hint.getType() == HintType.USE_SCROLL) {
				scrollHint = hint;
				break;
			}
		}
		if (scrollHint != null) {
			int scrollSize = (Integer) scrollHint.getParams()[0];
			int timeoutInMilli = (Integer) scrollHint.getParams()[1];

			//zhongshu-comment 假如我的sql没有order by的话，就默认按"_doc"字段升序排列
			//zhongshu-comment 那隐含意思是不是使用scroll获取数据一定要排序？
			if (!existsOrderBy)
				request.addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC);
			request.setScroll(new TimeValue(timeoutInMilli)).setSize(scrollSize);
		}
		return scrollHint != null;
	}

	/**
	 * Set indices and types to the search request.
	 */
	private void setIndicesAndTypes() {
		request.setIndices(query.getIndexArr());

		String[] typeArr = query.getTypeArr();
		if (typeArr != null) {
			request.setTypes(typeArr);
		}
	}

	/**
	 * Set source filtering on a search request.
	 * zhongshu-comment 即es dsl中的include和exclude
	 * @param fields
	 *            list of fields to source filter.
	 */
	public void setFields(List<Field> fields) throws SqlParseException {
		/*
		zhongshu-comment select * from tbl_a;
		select * 这种sql语句的select.getFields().size()为0
		 */
		if (select.getFields().size() > 0) {
			ArrayList<String> includeFields = new ArrayList<String>();
			ArrayList<String> excludeFields = new ArrayList<String>();

			for (Field field : fields) {
				if (field instanceof MethodField) {
					MethodField method = (MethodField) field;
					if (method.getName().toLowerCase().equals("script")) {
						/*
						zhongshu-comment scripted_field only allows script(name,script) or script(name,lang,script)
						script类型的MethodField是不会加到include和exclude中的
						 */
						handleScriptField(method);
					} else if (method.getName().equalsIgnoreCase("include")) {
						for (KVValue kvValue : method.getParams()) {
							//zhongshu-comment select a,b,c 中的a、b、c字段add到includeFields中
							includeFields.add(kvValue.value.toString()) ;
						}
					} else if (method.getName().equalsIgnoreCase("exclude")) {
						for (KVValue kvValue : method.getParams()) {
							excludeFields.add(kvValue.value.toString()) ;
						}
					}
				} else if (field instanceof Field) {
					includeFields.add(field.getName());
				}
			}

			request.setFetchSource(
			        includeFields.toArray(new String[includeFields.size()]),
                    excludeFields.toArray(new String[excludeFields.size()])
            );
		}
	}

	/**
	 * zhongshu-comment scripted_field only allows script(name,script) or script(name,lang,script)
	 * @param method
	 * @throws SqlParseException
	 */
	private void handleScriptField(MethodField method) throws SqlParseException {
		List<KVValue> params = method.getParams();
		if (params.size() == 2) {
			request.addScriptField(params.get(0).value.toString(), new Script(params.get(1).value.toString()));
		} else if (params.size() == 3) {
			request.addScriptField(params.get(0).value.toString(),
                    new Script(
                            ScriptType.INLINE,
                            params.get(1).value.toString(),
                            params.get(2).value.toString(),
                            Collections.emptyMap()
                    )
            );
		} else {
			throw new SqlParseException("scripted_field only allows script(name,script) or script(name,lang,script)");
		}
	}

	/**
	 * Create filters or queries based on the Where clause.
	 * 
	 * @param where
	 *            the 'WHERE' part of the SQL query.
	 * @throws SqlParseException
	 */
	private void setWhere(Where where) throws SqlParseException {
		if (where != null) {
			BoolQueryBuilder boolQuery = QueryMaker.explan(where,this.select.isQuery);
			request.setQuery(boolQuery);
		}
	}

	/**
	 * Add sorts to the elasticsearch query based on the 'ORDER BY' clause.
	 * 
	 * @param orderBys
	 *            list of Order object
	 */
	private void setSorts(List<Order> orderBys) {
		for (Order order : orderBys) {
            if (order.getNestedPath() != null) {
                request.addSort(
                		SortBuilders.fieldSort(order.getName())
								.order(SortOrder.valueOf(order.getType()))
								.setNestedSort(new NestedSortBuilder(order.getNestedPath()))
				);
            } else if (order.getName().contains("script(")) { //zhongshu-comment 该分支是我后来加的，用于兼容order by case when那种情况
				String scriptStr = order.getName().substring("script(".length(), order.getName().length() - 1);
				Script script = new Script(scriptStr);
				ScriptSortBuilder scriptSortBuilder = SortBuilders.scriptSort(script, ScriptSortBuilder.ScriptSortType.NUMBER);
				scriptSortBuilder = scriptSortBuilder.order(SortOrder.valueOf(order.getType()));
				request.addSort(scriptSortBuilder);
			} else {
                request.addSort(
                		order.getName(),
						SortOrder.valueOf(order.getType()));
            }
		}
	}

	/**
	 * Add from and size to the ES query based on the 'LIMIT' clause
	 * 
	 * @param from
	 *            starts from document at position from
	 * @param size
	 *            number of documents to return.
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
