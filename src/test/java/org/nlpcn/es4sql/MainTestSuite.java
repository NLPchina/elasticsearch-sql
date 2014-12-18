package org.nlpcn.es4sql;



import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.io.ByteStreams;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
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
		AggregationTest.class
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

		loadAllData();

		searchDao = new SearchDao(client);
		System.out.println("Finished the setup process...");
	}


	@AfterClass
	public static void tearDown() {
		System.out.println("teardown process...");
	}


	public static SearchDao getSearchDao() {
		return searchDao;
	}


	private static InetSocketTransportAddress getTransportAddress() {
		String host = System.getenv("ES_TEST_HOST");
		String port = System.getenv("ES_TEST_PORT");

		if(host == null || port == null) {
			host = "localhost";
			port = "9300";

			System.out.println("ES_TEST_HOST AND ES_TEST_PORT enviroment variables does not exist.");
			System.out.println(String.format("Using defaults. host: %s. port:%s.", host, port));
		}

		return new InetSocketTransportAddress(host, Integer.parseInt(port));
	}


	private static void loadAllData() throws Exception {
		deleteIndex(TEST_INDEX);

		// load data
		BulkRequestBuilder bulkBuilder = new BulkRequestBuilder(client);
		loadBulk("src/test/resources/accounts.json", bulkBuilder);
		loadBulk("src/test/resources/phrases.json", bulkBuilder);
		loadBulk("src/test/resources/online.json", bulkBuilder);
		BulkResponse response = bulkBuilder.get();
		if(response.hasFailures()) {
			throw new Exception(String.format("Failed during bulk load. failure message: %s", response.buildFailureMessage()));
		}
		else {
			System.out.println("Loaded all test data into elasticsearch cluster");
		}
	}

	private static void deleteIndex(String indexName) {
		// delete index if exists.
		try {
			DeleteIndexResponse response = new DeleteIndexRequestBuilder(client.admin().indices(), indexName).get();
			if(response.isAcknowledged()) {
				System.out.println(String.format("Deleted index %s...", indexName));
			}
		}
		catch(IndexMissingException ex) {
			System.out.println(String.format("Cannot delete index %s. (index is missing). continue anyway...", indexName));
		}
	}


	private static void loadBulk(String jsonPath, BulkRequestBuilder bulkBuilder) throws Exception {
		System.out.println(String.format("Loading file %s into elasticsearch cluster", jsonPath));
		byte[] buffer = ByteStreams.toByteArray(new FileInputStream(jsonPath));
		bulkBuilder.add(buffer, 0, buffer.length, true, TEST_INDEX, null);
	}
}
