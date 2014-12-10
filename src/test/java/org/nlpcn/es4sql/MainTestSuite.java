package org.nlpcn.es4sql;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.exists.ExistsRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.common.io.ByteStreams;
import org.elasticsearch.common.io.BytesStream;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import sun.misc.IOUtils;

import java.io.*;

import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;


@RunWith(Suite.class)
@Suite.SuiteClasses({
		QueryTest.class,
		MethodQueryTest.class,
		AggregationTest.class
})
public class MainTestSuite {

	public final static String TEST_INDEX = "elasticsearch-sql_test_index";

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
		// delete index if exists.
		client.delete(new DeleteRequest(TEST_INDEX));

		// load data
		BulkRequestBuilder bulkBuilder = new BulkRequestBuilder(client);
		loadBulk("src/test/resource/accounts.json", bulkBuilder);
		BulkResponse response = bulkBuilder.get();
		if(response.hasFailures()) {
			throw new Exception(String.format("Failed during bulk load. failure message: %s", response.buildFailureMessage()));
		}
		else {
			System.out.println("Loaded all test data into elasticsearch cluster");
		}
	}


	private static void loadBulk(String jsonPath, BulkRequestBuilder bulkBuilder) throws Exception {
		byte[] buffer = ByteStreams.toByteArray(new FileInputStream(jsonPath));
		bulkBuilder.add(buffer, 0, buffer.length, true, TEST_INDEX, null);
	}
}
