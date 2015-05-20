package org.nlpcn.es4sql;



import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.io.ByteStreams;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndexMissingException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.io.FileInputStream;
import static org.nlpcn.es4sql.TestsConstants.*;

@RunWith(Suite.class)
@Suite.SuiteClasses({
		QueryTest.class,
		MethodQueryTest.class,
		AggregationTest.class,
		BugTest.class,
		DeleteTest.class,
		ExplainTest.class
})
public class MainTestSuite {

	private static TransportClient client;
	private static SearchDao searchDao;

	@BeforeClass
	public static void setUp() throws Exception {
		client = new TransportClient();
		client.addTransportAddress(getTransportAddress());

		NodesInfoResponse nodeInfos = client.admin().cluster().prepareNodesInfo().get();
		String clusterName = nodeInfos.getClusterName().value();
		System.out.println(String.format("Found cluster... cluster name: %s", clusterName));

		// Load test data.
		deleteQuery(TEST_INDEX);
		loadBulk("src/test/resources/accounts.json");
		loadBulk("src/test/resources/online.json");
        loadBulk("src/test/resources/phrases.json");

        prepareOdbcIndex();
        loadBulk("src/test/resources/odbc-date-formats.json");

		searchDao = new SearchDao(client);
		System.out.println("Finished the setup process...");
	}


	@AfterClass
	public static void tearDown() {
		System.out.println("teardown process...");
	}


	/**
	 * Delete all data inside specific index
	 * @param indexName the documents inside this index will be deleted.
	 */
	public static void deleteQuery(String indexName) {
		deleteQuery(indexName, null);
	}

	/**
	 * Delete all data using DeleteByQuery.
	 * @param indexName the index to delete
	 * @param typeName the type to delete
	 */
	public static void deleteQuery(String indexName, String typeName) {
		try {
			DeleteByQueryRequestBuilder deleteQuery = new DeleteByQueryRequestBuilder(client);
			deleteQuery.setIndices(indexName);
			if (typeName != null) {
				deleteQuery.setTypes(typeName);
			}
			deleteQuery.setQuery(QueryBuilders.matchAllQuery());

			deleteQuery.get();
			System.out.println(String.format("Deleted index %s and type %s", indexName, typeName));
		}
		catch(IndexMissingException e) {
			System.out.println(String.format("Failed to delete index, Index %s does not exist, continue any way", indexName));
		}
	}


	/**
	 * Loads all data from the json into the test
	 * elasticsearch cluster, using TEST_INDEX
	 * @param jsonPath the json file represents the bulk
	 * @throws Exception
	 */
	public static void loadBulk(String jsonPath) throws Exception {
		System.out.println(String.format("Loading file %s into elasticsearch cluster", jsonPath));

		BulkRequestBuilder bulkBuilder = new BulkRequestBuilder(client);
		byte[] buffer = ByteStreams.toByteArray(new FileInputStream(jsonPath));
		bulkBuilder.add(buffer, 0, buffer.length, true, TEST_INDEX, null);
		BulkResponse response = bulkBuilder.get();

		if(response.hasFailures()) {
			throw new Exception(String.format("Failed during bulk load of file %s. failure message: %s", jsonPath, response.buildFailureMessage()));
		}
	}

    public static void prepareOdbcIndex(){
        String dataMapping = "{\n" +
                "\t\"odbc\" :{\n" +
                "\t\t\"properties\":{\n" +
                "\t\t\t\"insert_time\":{\n" +
                "\t\t\t\t\"type\":\"date\",\n" +
                "\t\t\t\t\"format\": \"{'ts' ''yyyy-MM-dd HH:mm:ss.SSS''}\"\n" +
                "\t\t\t},\n" +
                "\t\t\t\"docCount\":{\n" +
                "\t\t\t\t\"type\":\"string\"\n" +
                "\t\t\t}\n" +
                "\t\t}\n" +
                "\t}\n" +
                "}";

        client.admin().indices().preparePutMapping(TEST_INDEX).setType("odbc").setSource(dataMapping).execute().actionGet();
    }

	public static SearchDao getSearchDao() {
		return searchDao;
	}

	public static TransportClient getClient() {
		return client;
	}

	private static InetSocketTransportAddress getTransportAddress() {
		String host = System.getenv("ES_TEST_HOST");
		String port = System.getenv("ES_TEST_PORT");

		if(host == null) {
			host = "localhost";
			System.out.println("ES_TEST_HOST enviroment variable does not exist. choose default 'localhost'");
		}

		if(port == null) {
			port = "9300";
			System.out.println("ES_TEST_PORT enviroment variable does not exist. choose default '9300'");
		}

		System.out.println(String.format("Connection details: host: %s. port:%s.", host, port));
		return new InetSocketTransportAddress(host, Integer.parseInt(port));
	}


}
