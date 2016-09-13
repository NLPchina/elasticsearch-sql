package org.elasticsearch.plugin.nlpcn;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHits;
import org.nlpcn.es4sql.SearchDao;
import org.nlpcn.es4sql.query.SqlElasticSearchRequestBuilder;

public class Test {
	public static void main(String[] args) throws Exception {
		
		Client client = null;
		
		String cluster = "elasticsearch";
		Node node = NodeBuilder.nodeBuilder()
				.clusterName(cluster)
				.client(true)
				.node();
		client = node.client();
		
		SearchDao searchDao = new SearchDao(client);
		
		String query = "select count(*) from abc group by id";
        SqlElasticSearchRequestBuilder select = (SqlElasticSearchRequestBuilder) searchDao.explain(query).explain();
        
        System.out.println(select.toString());
        
        SearchResponse searchResponse = (SearchResponse)select.get();
        
        System.out.println(searchResponse.toString());
        
        SearchHits result = searchResponse.getHits();
        
        System.out.println(result.getHits().length);
	}
}
