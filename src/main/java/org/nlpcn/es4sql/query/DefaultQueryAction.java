package org.nlpcn.es4sql.query;

import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.ScriptSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.nlpcn.es4sql.domain.Field;
import org.nlpcn.es4sql.domain.KVValue;
import org.nlpcn.es4sql.domain.MethodField;
import org.nlpcn.es4sql.domain.Order;
import org.nlpcn.es4sql.domain.Select;
import org.nlpcn.es4sql.domain.Where;
import org.nlpcn.es4sql.domain.hints.Hint;
import org.nlpcn.es4sql.domain.hints.HintType;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.maker.QueryMaker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Transform SQL query to standard Elasticsearch search query
 */
public class DefaultQueryAction extends QueryAction {

	private final Select select;
	private SearchRequestBuilder request;

    private List<String> fieldNames = new LinkedList<>();

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
        Hint scrollHint = null;
        for (Hint hint : select.getHints()) {
            if (hint.getType() == HintType.USE_SCROLL) {
                scrollHint = hint;
                break;
            }
        }
        if (scrollHint != null && scrollHint.getParams()[0] instanceof String) {
            return new SqlElasticSearchRequestBuilder(new SearchScrollRequestBuilder(client, SearchScrollAction.INSTANCE, (String) scrollHint.getParams()[0]).setScroll(new TimeValue((Integer) scrollHint.getParams()[1])));
        }

		/*
		zhongshu-comment 6.1.1.5这个版本和elastic6.1.1这个分支用的是这一行代码
		但是在本地调试时我的client没有实例化，并没有去连es，所以这行代码会报空指针
		那就将这行注释掉吧，以后就用下面那行
		 */
//		this.request = client.prepareSearch();

		/*
		zhongshu-comment  6.2.4.1这个版本和master_zhongshu_dev_01用的是这一行代码，虽然client为null，但是下面这行代码并不会报空指针
							为了在本地调试、执行下文的那些代码获得es的dsl，所以就使用这行代码，暂时将上面哪一行注释掉，上线的时候记得替换掉
		变量request是es搜索请求对象，调用的是es的api，SearchRequestBuilder是es的原生api
		 */
        this.request = new SearchRequestBuilder(client, SearchAction.INSTANCE);
		setIndicesAndTypes();

		//zhongshu-comment 将Select对象中封装的sql token信息转换并传到成员变量es搜索请求对象request中
		setFields(select.getFields());

		setWhere(select.getWhere());
		setSorts(select.getOrderBys());
		setLimit(select.getOffset(), select.getRowCount());

        //
        if (scrollHint != null) {
            if (!select.isOrderdSelect())
                request.addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC);
            request.setSize((Integer) scrollHint.getParams()[0]).setScroll(new TimeValue((Integer) scrollHint.getParams()[1]));
        } else {
            request.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
        }
        updateRequestWithIndexAndRoutingOptions(select, request);
		updateRequestWithHighlight(select, request);
		updateRequestWithCollapse(select, request);
		updateRequestWithPostFilter(select, request);
		updateRequestWithStats(select, request);
		updateRequestWithPreference(select, request);
		updateRequestWithTrackTotalHits(select, request);
		updateRequestWithTimeout(select, request);
		updateRequestWithIndicesOptions(select, request);
		updateRequestWithMinScore(select, request);
		updateRequestWithSearchAfter(select, request);
		updateRequestWithRuntimeMappings(select, request);
		SqlElasticSearchRequestBuilder sqlElasticRequestBuilder = new SqlElasticSearchRequestBuilder(request);

		return sqlElasticRequestBuilder;
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
				if (field instanceof MethodField) { //zhongshu-comment MethodField是Field的子类，而且Field也就只有MethodField这一个子类了
					MethodField method = (MethodField) field;
					if (method.getName().toLowerCase().equals("script")) {
						/*
						zhongshu-comment scripted_field only allows script(name,script) or script(name,lang,script)
						script类型的MethodField是不会加到include和exclude中的
						 */
						handleScriptField(method);
					} else if (method.getName().equalsIgnoreCase("include")) {
					    String f;
						for (KVValue kvValue : method.getParams()) {
							//zhongshu-comment select a,b,c 中的a、b、c字段add到includeFields中
                            f = kvValue.value.toString();
                            fieldNames.add(f);
                            includeFields.add(f);
						}
					} else if (method.getName().equalsIgnoreCase("exclude")) {
						for (KVValue kvValue : method.getParams()) {
							excludeFields.add(kvValue.value.toString()) ;
						}
					} else if (method.getName().equalsIgnoreCase("docvalue")) {
						handleDocvalueField(method);
					}
				} else if (field != null) {
                    fieldNames.add(field.getName());
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
            String f = params.get(0).value.toString();
            fieldNames.add(f);
            request.addScriptField(f, new Script(params.get(1).value.toString()));
        } else if (params.size() == 3) {
            String f = params.get(0).value.toString();
            fieldNames.add(f);
            request.addScriptField(f,
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

	private void handleDocvalueField(MethodField method) throws SqlParseException {
		List<KVValue> params = method.getParams();
		if (params.size() == 1) {
			String f = params.get(0).value.toString();
			fieldNames.add(f);
			request.addDocValueField(f);
		} else if (params.size() == 2) {
			String f = params.get(0).value.toString();
			fieldNames.add(f);
			request.addDocValueField(f, params.get(1).value.toString());
		} else {
			throw new SqlParseException("docvalue_fields only allows docvalue(field) or docvalue(field,format)");
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
			if (order.getName().equals(ScoreSortBuilder.NAME)) {
				request.addSort(SortBuilders.scoreSort().order(SortOrder.valueOf(order.getType())));
            } else if (order.getName().contains("script(")) { //zhongshu-comment 该分支是我后来加的，用于兼容order by case when那种情况

				String scriptStr = order.getName().substring("script(".length(), order.getName().length() - 1);
				Script script = new Script(scriptStr);
				ScriptSortBuilder scriptSortBuilder = SortBuilders.scriptSort(script, order.getScriptSortType());

				scriptSortBuilder = scriptSortBuilder.order(SortOrder.valueOf(order.getType()));
				request.addSort(scriptSortBuilder);
			} else {
				FieldSortBuilder fieldSortBuilder = SortBuilders.fieldSort(order.getName()).order(SortOrder.valueOf(order.getType()));
				if (order.getNestedPath() != null) {
					fieldSortBuilder.setNestedSort(new NestedSortBuilder(order.getNestedPath()));
				}
				if (order.getMissing() != null) {
					fieldSortBuilder.missing(order.getMissing());
				}
				if (order.getUnmappedType() != null) {
					fieldSortBuilder.unmappedType(order.getUnmappedType());
				}
				if (order.getNumericType() != null) {
					fieldSortBuilder.setNumericType(order.getNumericType());
				}
				if (order.getFormat() != null) {
					fieldSortBuilder.setFormat(order.getFormat());
				}
				request.addSort(fieldSortBuilder);
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

    public List<String> getFieldNames() {
        return fieldNames;
    }
}
