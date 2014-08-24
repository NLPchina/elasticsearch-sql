package org.nlpcn.es4sql;

import java.io.IOException;

import org.apache.lucene.queryparser.xml.builders.SpanQueryBuilderFactory;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SpanTermQueryBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.nlpcn.es4sql.domain.Order;
import org.nlpcn.es4sql.domain.SearchResult;
import org.nlpcn.es4sql.domain.Select;
import org.nlpcn.es4sql.domain.Where;
import org.nlpcn.es4sql.exception.SqlParseException;

public class SearchDao {

	private Client client = null;

	@SuppressWarnings("resource")
	public SearchDao(String ip, int port) {
		this.client = new TransportClient().addTransportAddress(new InetSocketTransportAddress(ip, port));
	}

	/**
	 * 查询返回一个初步的封装结果
	 * 
	 * @param sql
	 * @return
	 * @throws IOException
	 * @throws SqlParseException
	 */
	public SearchResult selectAsResult(String sql) throws IOException, SqlParseException {
		return new SearchResult(select(sql));
	}

	/**
	 * 查询返回es的查询结果
	 * 
	 * @param sql
	 * @return
	 * @throws IOException
	 * @throws SqlParseException
	 */
	public SearchResponse select(String sql) throws IOException, SqlParseException {

		Select select = new SqlParser().parseSelect(sql);

		// set index
		SearchRequestBuilder request = client.prepareSearch(select.getIndexArr());

		// set type
		String[] typeArr = select.getTypeArr();
		if (typeArr != null) {
			request.setTypes(typeArr);
		}

		// set where
		Where where = select.getWhere();
		if (where != null) {
			if (select.isQuery) {
				BoolQueryBuilder boolQuery = QueryMaker.explan(where);
				request.setQuery(boolQuery);
			} else {
				BoolFilterBuilder boolFilter = FilterMaker.explan(where);
				request.setPostFilter(boolFilter);
			}
		}

		

		request.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);

		request.setFrom(select.getOffset());

		if (select.getRowCount() > -1) {
			request.setSize(select.getRowCount());
		}

		if (select.getFields().size() > 0) {
			request.addFields(select.getFieldArr());
		}

		// add order
		for (Order order : select.getOrderBys()) {
			request.addSort(order.getName(), SortOrder.valueOf(order.getType()));
		}
		
		
		System.out.println(request);
		
		return request.execute().actionGet();
	}

	public static void main(String[] args) throws IOException, SqlParseException {
		// SearchDao searchDao = new SearchDao("192.168.200.63", 9300);
		SearchDao searchDao = new SearchDao("localhost", 9300);
		String query = "select title,crawlid from doc " + "where " + "( title = matchPhraseQuery('中国','default',100)) order by _score desc " + " limit 3";
		SearchResponse searchByQuery = searchDao.select(query);

		System.out.println(new SearchResult(searchByQuery));
		System.out.println(searchByQuery);
	}
}
