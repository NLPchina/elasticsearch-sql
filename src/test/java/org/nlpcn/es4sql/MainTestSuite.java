package org.nlpcn.es4sql;

import junit.framework.TestCase;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import java.net.InetSocketAddress;


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
	public static void setUp() {
		client = new TransportClient();
		client.addTransportAddress(getTransportAddress());

		NodesInfoResponse nodeInfos = client.admin().cluster().prepareNodesInfo().get();
		String clusterName = nodeInfos.getClusterName().value();
		System.out.println(String.format("Found cluster... cluster name: %s", clusterName));

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
}
