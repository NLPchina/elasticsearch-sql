package org.nlpcn.es4sql;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.xml.FilterBuilderFactory;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest.OpType;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.search.MatchQuery;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTermsBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.AvgBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric.Script;
import org.elasticsearch.search.facet.histogram.HistogramFacetBuilder;
import org.elasticsearch.search.facet.terms.TermsFacetBuilder;
import org.nlpcn.commons.lang.util.StringUtil;

public class ESClient {
	private Client client = null;

	/**
	 * 初始化索引库客户端
	 * 
	 * @param ip
	 *            ip
	 * @param port
	 *            端口号
	 * @return
	 */
	public static ESClient initClient(String ip, int port) {
		ESClient su = new ESClient();
		TransportClient client = new TransportClient();
		client.addTransportAddress(new InetSocketTransportAddress(ip, port));
		su.setClient(client);
		return su;
	}

	/**
	 * 创建索引库
	 * 
	 * @param name
	 *            索引库的名字
	 * @param type
	 *            索引库中的文档类型
	 * @return
	 */
	public IndexResponse createIndex(String name, String type) {
		IndexRequestBuilder irb = new IndexRequestBuilder(client);
		IndexResponse actionGet = irb.setIndex(name).setType(type).setOpType(OpType.CREATE).setCreate(true).execute().actionGet();
		return actionGet;
	}

	/**
	 * 入索引
	 * 
	 * @param name
	 *            索引库名字
	 * @param type
	 *            索引库类型
	 * @param sourceBuilder
	 *            索引数据builder对象
	 * @return
	 */
	public IndexResponse index(String name, String type, String id, XContentBuilder sourceBuilder) {
		IndexRequestBuilder irb = new IndexRequestBuilder(client);
		IndexResponse actionGet = irb.setIndex(name).setType(type).setId(id).setSource(sourceBuilder).setOpType(OpType.INDEX).execute().actionGet();
		return actionGet;
	}

	/**
	 * 入索引
	 * 
	 * @param name
	 *            索引库名字
	 * @param type
	 *            索引库类型
	 * @param map
	 *            入索引数据
	 * @return
	 * @throws IOException
	 */
	public IndexResponse index(String name, String type, String id, Map<String, String> map) throws IOException {
		XContentBuilder startObject = XContentFactory.jsonBuilder().startObject();
		for (String str : map.keySet()) {
			startObject.field(str, map.get(str));
		}
		startObject.endObject();
		IndexResponse actionGet = index(name, type, id, startObject);
		return actionGet;
	}

	/**
	 * 删除索引库
	 * 
	 * @param name
	 *            要删除的索引库
	 * @param type
	 *            要删除的库中的类型
	 * @return
	 */
	public DeleteResponse deleteIndex(String name, String type) {
		// -TODO 删除index or type
		// DeleteResponse actionGet = db.setIndex(name).execute().actionGet();
		return null;
	}

	/**
	 * 迭代查询
	 * 
	 * @param indexName
	 * @param typeName
	 * @param query
	 * @param size
	 * @return
	 */
	public Iterater iterater(String indexName, String typeName, String query, int size) {
		return new Iterater(indexName, typeName, query, size);
	}

	public class Iterater {
		SearchResponse scrollResp = null;

		/**
		 * 迭代查询指定索引库中的指定类型的文档,和searchScroll配合使用
		 * 
		 * @param indexName
		 *            索引库名字
		 * @param typeName
		 *            索引库中的文档类型
		 * @param size
		 *            指定个数
		 * @return
		 */
		public Iterater(String indexName, String typeName, String query, int size) {
			SearchRequestBuilder srb = client.prepareSearch(indexName, typeName).setSearchType(SearchType.SCAN).setScroll(new TimeValue(60000)).setSize(size);
			if (StringUtil.isNotBlank(query)) {
				srb.setQuery(query);
			}
			scrollResp = srb.execute().actionGet();
		}

		/**
		 * 迭代方式查询索引库，和scrollDocs配合使用
		 * 
		 * @return 查询结果
		 */
		public SearchResponse next() {
			if (scrollResp != null) {
				scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
				if (scrollResp.getHits().getHits().length == 0) {
					return null;
				}
			}
			return scrollResp;
		}
	}

	// sum
	public SearchResponse sum(String indexName, String typeName, String sumName, String key) {
		SumBuilder sum = AggregationBuilders.sum(sumName).field(key);
		SearchResponse actionGet = client.prepareSearch(indexName).setTypes(typeName).addAggregation(sum).setSize(0).execute().actionGet();
		return actionGet;
	}

	// term facet
	public SearchResponse termFacet(String indexName, String typeName, String name, String field) {
		TermsBuilder field2 = AggregationBuilders.terms(name).field(field);
		// field2.size(20);
		// field2.lang("groovy");
		// field2.param("dd", 1);
		// field2.script("doc['age']==32?3:4") ;

		System.out.println(client.prepareSearch(indexName).setTypes(typeName).setSize(0).addAggregation(field2));

		SearchResponse actionGet = client.prepareSearch(indexName).setTypes(typeName).setSize(0).addAggregation(field2).execute().actionGet();
		return actionGet;
	}

	// count
	public CountResponse count(String indexName, String typeName, String countName, String key, Object value) {
		MatchQueryBuilder matchQuery = QueryBuilders.matchQuery(key, value);
		// TermQueryBuilder tqb = QueryBuilders.termQuery(key, value);
		CountResponse actionGet = client.prepareCount().setIndices(indexName).setTypes(typeName).setQuery(matchQuery).execute().actionGet();
		return actionGet;
	}

	// order by
	public void orderBy(String indexName, String typeName, String orderByName, String key) {

	}

	public SearchResponse search(String indexName, String typeName, String query, int size) {
		QueryBuilder qb = QueryBuilders.queryString(query);
		SearchRequestBuilder setQuery = client.prepareSearch(indexName).setTypes(typeName).setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(qb).setSize(size);
		return setQuery.execute().actionGet();
	}

	public SearchResponse search(String indexName, String typeName, String key, String value, int size) {
		QueryBuilder qb = QueryBuilders.matchQuery(key, value);
		SearchRequestBuilder setQuery = client.prepareSearch(indexName).setTypes(typeName).setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(qb).setSize(size);
		return setQuery.execute().actionGet();
	}

	public SearchResponse search(String[] indexName, String[] typeName, QueryBuilder qb, int size) {
		SearchRequestBuilder setQuery = client.prepareSearch(indexName).setTypes(typeName).setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(qb).setSize(size);
		return setQuery.execute().actionGet();
	}

	public Client getClient() {
		return client;
	}

	// 关闭客户端连接
	public void close() {
		client.close();
	}

	public void setClient(Client client) {
		this.client = client;
	}

	public static void main(String[] args) {
		String url = "192.168.200.19";
		int port = 9300;
		ESClient esc = ESClient.initClient(url, port);
		String indexName = "baike";
		String indexType = "soso";
		System.out.println(esc.search("baike", "soso", "name:a", 10));
		// DeleteResponse deleteIndex = esc.deleteIndex(indexName, indexType);
		// System.err.println(deleteIndex.toString());
	}

	public static String searchVideo(String names, int size) {
		String VIDEO = "resource_video";
		String url = "172.21.19.239";
		int port = 9300;
		ESClient sc = ESClient.initClient(url, port);
		QueryStringQueryBuilder qsqb = new QueryStringQueryBuilder(QueryParser.escape(names));
		qsqb.field("name").field("alias");
		SearchResponse search = sc.search(new String[] { VIDEO }, new String[] { VIDEO }, qsqb, size);
		return search.toString();
	}
}
