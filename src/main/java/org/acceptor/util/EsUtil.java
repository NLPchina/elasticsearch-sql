package org.acceptor.util;

import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;
import java.util.WeakHashMap;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.nlpcn.es4sql.SearchDao;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.SqlElasticSearchRequestBuilder;

public class EsUtil {
	
	private static Map<String, Client> clientCache = new WeakHashMap<String, Client>();
	
	public static synchronized Client getClient(String cluster, String ip) {
		Client client = clientCache.get(ip);
		if(client == null) {
			client = initClient(cluster, ip);
			clientCache.put(ip, client);
		}
		return client;
	}
	
	private static Client initClient(String cluster, String ip) {
		Node node = NodeBuilder.nodeBuilder()
				.clusterName(cluster)
				.client(true)
				.node();
		return node.client();
	}
	
	public static String explain(String sql, String cluster, String ip) throws SQLFeatureNotSupportedException, SqlParseException {
		Client client = getClient(cluster, ip);
		SearchDao searchDao = new SearchDao(client);
		SqlElasticSearchRequestBuilder select = (SqlElasticSearchRequestBuilder) searchDao.explain(sql).explain();
		return select.toString();
	}
	
	public static String execGet(String sql, String cluster, String ip) throws SQLFeatureNotSupportedException, SqlParseException {
		Client client = getClient(cluster, ip);
		SearchDao searchDao = new SearchDao(client);
		SqlElasticSearchRequestBuilder select = (SqlElasticSearchRequestBuilder) searchDao.explain(sql).explain();
		SearchResponse searchResponse = (SearchResponse)select.get();
		return searchResponse.toString();
	}
	
}
