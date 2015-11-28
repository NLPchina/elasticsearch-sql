package org.nlpcn.es4sql;



import com.google.common.io.ByteStreams;
import org.apache.lucene.index.IndexNotFoundException;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryAction;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;

import org.elasticsearch.plugin.deletebyquery.DeleteByQueryPlugin;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.io.FileInputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.nlpcn.es4sql.TestsConstants.*;

@RunWith(Suite.class)
@Suite.SuiteClasses({
		QueryTest.class,
		MethodQueryTest.class,
		AggregationTest.class,
		BugTest.class,
        JoinTests.class,
		DeleteTest.class,
		ExplainTest.class,
        WktToGeoJsonConverterTests.class,
        SqlParserTests.class,
        ShowTest.class
})
public class MainTestSuite {


	private static TransportClient client;
	private static SearchDao searchDao;

	@BeforeClass
	public static void setUp() throws Exception {

        Settings settings = Settings.builder().put("plugin.types", DeleteByQueryPlugin.class.getName()).build();
        client = TransportClient.builder().settings(settings).build().addTransportAddress(getTransportAddress());

		NodesInfoResponse nodeInfos = client.admin().cluster().prepareNodesInfo().get();
		String clusterName = nodeInfos.getClusterName().value();
		System.out.println(String.format("Found cluster... cluster name: %s", clusterName));

		// Load test data.
        if(client.admin().indices().prepareExists(TEST_INDEX).execute().actionGet().isExists()){
            client.admin().indices().prepareDelete(TEST_INDEX).get();
        }
		loadBulk("src/test/resources/accounts.json");
		loadBulk("src/test/resources/online.json");
        loadBulk("src/test/resources/phrases.json");
        loadBulk("src/test/resources/dogs.json");
        loadBulk("src/test/resources/peoples.json");
        loadBulk("src/test/resources/game_of_thrones_complex.json");

        prepareOdbcIndex();
        loadBulk("src/test/resources/odbc-date-formats.json");

        prepareSpatialIndex("location");
        loadBulk("src/test/resources/locations.json");

        prepareSpatialIndex("location2");
        loadBulk("src/test/resources/locations2.json");

        prepareNestedTypeIndex();
        loadBulk("src/test/resources/nested_objects.json");

        searchDao = new SearchDao(client);

        //refresh to make sure all the docs will return on queries
        client.admin().indices().prepareRefresh(TEST_INDEX).execute().actionGet();

		System.out.println("Finished the setup process...");
	}

    private static void prepareNestedTypeIndex() {

            String dataMapping = "{ \"nestedType\": {\n" +
                    "        \"properties\": {\n" +
                    "          \"message\": {\n" +
                    "            \"type\": \"nested\",\n" +
                    "            \"properties\": {\n" +
                    "              \"info\": {\n" +
                    "                \"type\": \"string\",\n" +
                    "                \"index\": \"not_analyzed\"\n" +
                    "              }\n" +
                    "            }\n" +
                    "          },\n" +
                    "          \"myNum\": {\n" +
                    "            \"type\": \"long\"\n" +
                    "          }\n" +
                    "        }\n" +
                    "      }\n" +
                    "    }}";

            client.admin().indices().preparePutMapping(TEST_INDEX).setType("nestedType").setSource(dataMapping).execute().actionGet();
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

			DeleteByQueryRequestBuilder deleteQuery = new DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE);
			deleteQuery.setIndices(indexName);
			if (typeName != null) {
				deleteQuery.setTypes(typeName);
			}
			deleteQuery.setQuery(QueryBuilders.matchAllQuery());

			deleteQuery.get();
			System.out.println(String.format("Deleted index %s and type %s", indexName, typeName));


	}


	/**
	 * Loads all data from the json into the test
	 * elasticsearch cluster, using TEST_INDEX
	 * @param jsonPath the json file represents the bulk
	 * @throws Exception
	 */
	public static void loadBulk(String jsonPath) throws Exception {
		System.out.println(String.format("Loading file %s into elasticsearch cluster", jsonPath));

		BulkRequestBuilder bulkBuilder = client.prepareBulk();
		byte[] buffer = ByteStreams.toByteArray(new FileInputStream(jsonPath));
		bulkBuilder.add(buffer, 0, buffer.length, TEST_INDEX, null);
		BulkResponse response = bulkBuilder.get();

		if(response.hasFailures()) {
			throw new Exception(String.format("Failed during bulk load of file %s. failure message: %s", jsonPath, response.buildFailureMessage()));
		}
	}
    public static void prepareSpatialIndex(String type){
        String dataMapping = "{\n" +
                "\t\""+type+"\" :{\n" +
                "\t\t\"properties\":{\n" +
                "\t\t\t\"place\":{\n" +
                "\t\t\t\t\"type\":\"geo_shape\",\n" +
                "\t\t\t\t\"tree\": \"quadtree\",\n" +
                "\t\t\t\t\"precision\": \"10km\"\n" +
                "\t\t\t},\n" +
                "\t\t\t\"center\":{\n" +
                "\t\t\t\t\"type\":\"geo_point\",\n" +
                "\t\t\t\t\"geohash\":true,\n" +
                "\t\t\t\t\"geohash_prefix\":true,\n" +
                "\t\t\t\t\"geohash_precision\":17\n" +
                "\t\t\t},\n" +
                "\t\t\t\"description\":{\n" +
                "\t\t\t\t\"type\":\"string\"\n" +
                "\t\t\t}\n" +
                "\t\t}\n" +
                "\t}\n" +
                "}";

        client.admin().indices().preparePutMapping(TEST_INDEX).setType(type).setSource(dataMapping).execute().actionGet();
    }
    public static void prepareOdbcIndex(){
        String dataMapping = "{\n" +
                "\t\"odbc\" :{\n" +
                "\t\t\"properties\":{\n" +
                "\t\t\t\"odbc_time\":{\n" +
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

	private static InetSocketTransportAddress getTransportAddress() throws UnknownHostException {
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
		return new InetSocketTransportAddress(InetAddress.getByName(host), Integer.parseInt(port));
	}

}
