package org.nlpcn.es4sql;

import com.google.common.io.ByteStreams;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.io.FileInputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_DOG;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_GAME_OF_THRONES;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_JOIN_TYPE;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_LOCATION;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_LOCATION2;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_NESTED_TYPE;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_ODBC;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_ONLINE;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_PEOPLE;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_PHRASE;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_SYSTEM;

@RunWith(Suite.class)
@Suite.SuiteClasses({
		QueryTest.class,
		MethodQueryTest.class,
		AggregationTest.class,
        JoinTests.class,
		ExplainTest.class,
        WktToGeoJsonConverterTests.class,
        SqlParserTests.class,
        ShowTest.class,
        CSVResultsExtractorTests.class,
        SourceFieldTest.class,
		SQLFunctionsTest.class,
		JDBCTests.class,
        UtilTests.class,
        MultiQueryTests.class,
        DeleteTest.class
})
public class MainTestSuite {

	private static TransportClient client;
	private static SearchDao searchDao;

	@BeforeClass
	public static void setUp() throws Exception {
		Settings settings = Settings.builder().put("client.transport.ignore_cluster_name",true).build();
		client = new PreBuiltXPackTransportClient(settings).addTransportAddress(getTransportAddress());

        NodesInfoResponse nodeInfos = client.admin().cluster().prepareNodesInfo().get();
		String clusterName = nodeInfos.getClusterName().value();
		System.out.println(String.format("Found cluster... cluster name: %s", clusterName));

		// Load test data.
        loadBulk("src/test/resources/online.json", TEST_INDEX_ONLINE);

        createTestIndex(TEST_INDEX_ACCOUNT);
        prepareAccountsIndex();
		loadBulk("src/test/resources/accounts.json", TEST_INDEX_ACCOUNT);

        createTestIndex(TEST_INDEX_PHRASE);
		preparePhrasesIndex();
        loadBulk("src/test/resources/phrases.json", TEST_INDEX_PHRASE);

        createTestIndex(TEST_INDEX_DOG);
        prepareDogsIndex();
        loadBulk("src/test/resources/dogs.json", TEST_INDEX_DOG);

        createTestIndex(TEST_INDEX_PEOPLE);
        loadBulk("src/test/resources/peoples.json", TEST_INDEX_PEOPLE);

        createTestIndex(TEST_INDEX_GAME_OF_THRONES);
        prepareGameOfThronesIndex();
        loadBulk("src/test/resources/game_of_thrones_complex.json", TEST_INDEX_GAME_OF_THRONES);

        createTestIndex(TEST_INDEX_SYSTEM);
        loadBulk("src/test/resources/systems.json", TEST_INDEX_SYSTEM);

        createTestIndex(TEST_INDEX_ODBC);
        prepareOdbcIndex();
        loadBulk("src/test/resources/odbc-date-formats.json", TEST_INDEX_ODBC);

        createTestIndex(TEST_INDEX_LOCATION);
        prepareSpatialIndex(TEST_INDEX_LOCATION, "location");
        loadBulk("src/test/resources/locations.json", TEST_INDEX_LOCATION);

        createTestIndex(TEST_INDEX_LOCATION2);
        prepareSpatialIndex(TEST_INDEX_LOCATION2, "location2");
        loadBulk("src/test/resources/locations2.json", TEST_INDEX_LOCATION2);

        createTestIndex(TEST_INDEX_NESTED_TYPE);
        prepareNestedTypeIndex();
        loadBulk("src/test/resources/nested_objects.json", TEST_INDEX_NESTED_TYPE);

        createTestIndex(TEST_INDEX_JOIN_TYPE);
        prepareJoinTypeIndex();
        loadBulk("src/test/resources/join_objects.json", TEST_INDEX_JOIN_TYPE);

        searchDao = new SearchDao(client);

        //refresh to make sure all the docs will return on queries
        client.admin().indices().prepareRefresh(TEST_INDEX + "*").get();

		System.out.println("Finished the setup process...");
	}

    private static void createTestIndex(String index) {
        deleteTestIndex(index);
        client.admin().indices().prepareCreate(index).get();
    }

    private static void deleteTestIndex(String index) {
        if(client.admin().indices().prepareExists(index).get().isExists()){
            client.admin().indices().prepareDelete(index).get();
        }
    }

    private static void prepareGameOfThronesIndex() {
        String dataMapping = "{  \"gotCharacters\": { " +
                " \"properties\": {\n" +
                " \"nickname\": {\n" +
                "\"type\":\"text\", "+
                "\"fielddata\":true"+
                "},\n"+
                " \"name\": {\n" +
                "\"properties\": {\n" +
                "\"firstname\": {\n" +
                "\"type\": \"text\",\n" +
                "  \"fielddata\": true\n" +
                "},\n" +
                "\"lastname\": {\n" +
                "\"type\": \"text\",\n" +
                "  \"fielddata\": true\n" +
                "},\n" +
                "\"ofHerName\": {\n" +
                "\"type\": \"integer\"\n" +
                "},\n" +
                "\"ofHisName\": {\n" +
                "\"type\": \"integer\"\n" +
                "}\n" +
                "}\n" +
                "}"+
                "} } }";
        client.admin().indices().preparePutMapping(TEST_INDEX_GAME_OF_THRONES).setType("gotCharacters").setSource(dataMapping, XContentType.JSON).execute().actionGet();
    }

    private static void prepareDogsIndex() {
        String dataMapping = "{  \"dog\": {" +
                " \"properties\": {\n" +
                "          \"dog_name\": {\n" +
                "            \"type\": \"text\",\n" +
                "            \"fielddata\": true\n" +
                "          }"+
                "       }"+
                "   }" +
                "}";
        client.admin().indices().preparePutMapping(TEST_INDEX_DOG).setType("dog").setSource(dataMapping, XContentType.JSON).execute().actionGet();
    }

    private static void prepareAccountsIndex() {
        String dataMapping = "{  \"account\": {" +
                " \"properties\": {\n" +
                "          \"gender\": {\n" +
                "            \"type\": \"text\",\n" +
                "            \"fielddata\": true,\n" +
                "            \"fields\": {\n" +
                "              \"keyword\": {\n" +
                "                \"ignore_above\": 256,\n" +
                "                \"type\": \"keyword\"\n" +
                "              }\n" +
                "            }" +
                "          }," +
                "          \"address\": {\n" +
                "            \"type\": \"text\",\n" +
                "            \"fielddata\": true\n" +
                "          }," +
                "          \"state\": {\n" +
                "            \"type\": \"text\",\n" +
                "            \"fielddata\": true\n" +
                "          }" +
                "       }"+
                "   }" +
                "}";
        client.admin().indices().preparePutMapping(TEST_INDEX_ACCOUNT).setType("account").setSource(dataMapping, XContentType.JSON).execute().actionGet();
    }

    private static void preparePhrasesIndex() {
        String dataMapping = "{  \"phrase\": {" +
                " \"properties\": {\n" +
                "          \"phrase\": {\n" +
                "            \"type\": \"text\",\n" +
                "            \"store\": true\n" +
                "          }" +
                "       }"+
                "   }" +
                "}";
        client.admin().indices().preparePutMapping(TEST_INDEX_PHRASE).setType("phrase").setSource(dataMapping, XContentType.JSON).execute().actionGet();
    }

    private static void prepareNestedTypeIndex() {

            String dataMapping = "{ \"nestedType\": {\n" +
                    "        \"properties\": {\n" +
                    "          \"message\": {\n" +
                    "            \"type\": \"nested\",\n" +
                    "            \"properties\": {\n" +
                    "              \"info\": {\n" +
                    "                \"type\": \"keyword\",\n" +
                    "                \"index\": \"true\"\n" +
                    "              },\n" +
                    "              \"author\": {\n" +
                    "                \"type\": \"keyword\",\n" +
                    "                \"index\": \"true\"\n" +
                    "              },\n" +
                    "              \"dayOfWeek\": {\n" +
                    "                \"type\": \"long\"\n" +
                    "              }\n" +
                    "            }\n" +
                    "          },\n" +
                    "          \"comment\": {\n" +
                    "            \"type\": \"nested\",\n" +
                    "            \"properties\": {\n" +
                    "              \"data\": {\n" +
                    "                \"type\": \"keyword\",\n" +
                    "                \"index\": \"true\"\n" +
                    "              },\n" +
                    "              \"likes\": {\n" +
                    "                \"type\": \"long\"\n" +
                    "              }\n" +
                    "            }\n" +
                    "          },\n" +
                    "          \"myNum\": {\n" +
                    "            \"type\": \"long\"\n" +
                    "          },\n" +
                    "          \"someField\": {\n" +
                    "                \"type\": \"keyword\",\n" +
                    "                \"index\": \"true\"\n" +
                    "          }\n" +
                    "        }\n" +
                    "      }\n" +
                    "    }}";

            client.admin().indices().preparePutMapping(TEST_INDEX_NESTED_TYPE).setType("nestedType").setSource(dataMapping, XContentType.JSON).execute().actionGet();
    }

    private static void prepareJoinTypeIndex() {
        String dataMapping = "{\n" +
                "  \"joinType\": {\n" +
                "    \"properties\": {\n" +
                "      \"join_field\": {\n" +
                "        \"type\": \"join\",\n" +
                "        \"relations\": {\n" +
                "          \"parentType\": \"childrenType\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"parentTile\": {\n" +
                "        \"index\": \"true\",\n" +
                "        \"type\": \"keyword\"\n" +
                "      },\n" +
                "      \"dayOfWeek\": {\n" +
                "        \"type\": \"long\"\n" +
                "      },\n" +
                "      \"author\": {\n" +
                "        \"index\": \"true\",\n" +
                "        \"type\": \"keyword\"\n" +
                "      },\n" +
                "      \"info\": {\n" +
                "        \"index\": \"true\",\n" +
                "        \"type\": \"keyword\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        client.admin().indices().preparePutMapping(TEST_INDEX_JOIN_TYPE).setType("joinType").setSource(dataMapping, XContentType.JSON).execute().actionGet();
    }

    @AfterClass
	public static void tearDown() {
		System.out.println("teardown process...");

        deleteTestIndex(TEST_INDEX + "*");

		client.close();
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

        DeleteByQueryRequestBuilder deleteQueryBuilder = new DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE);
        deleteQueryBuilder.request().indices(indexName);
        if (typeName!=null) {
            deleteQueryBuilder.request().getSearchRequest().types(typeName);
        }
        deleteQueryBuilder.filter(QueryBuilders.matchAllQuery());
        deleteQueryBuilder.get();
        System.out.println(String.format("Deleted index %s and type %s", indexName, typeName));

    }


	/**
	 * Loads all data from the json into the test
	 * elasticsearch cluster, using TEST_INDEX
     * @param jsonPath the json file represents the bulk
     * @param defaultIndex
     * @throws Exception
	 */
	public static void loadBulk(String jsonPath, String defaultIndex) throws Exception {
		System.out.println(String.format("Loading file %s into elasticsearch cluster", jsonPath));

		BulkRequestBuilder bulkBuilder = client.prepareBulk();
		byte[] buffer = ByteStreams.toByteArray(new FileInputStream(jsonPath));
		bulkBuilder.add(buffer, 0, buffer.length, defaultIndex, XContentType.JSON);
		BulkResponse response = bulkBuilder.get();

		if(response.hasFailures()) {
			throw new Exception(String.format("Failed during bulk load of file %s. failure message: %s", jsonPath, response.buildFailureMessage()));
		}
	}

    public static void prepareSpatialIndex(String index, String type){
        String dataMapping = "{\n" +
                "\t\""+type+"\" :{\n" +
                "\t\t\"properties\":{\n" +
                "\t\t\t\"place\":{\n" +
                "\t\t\t\t\"type\":\"geo_shape\",\n" +
                "\t\t\t\t\"tree\": \"quadtree\",\n" +
                "\t\t\t\t\"precision\": \"10km\"\n" +
                "\t\t\t},\n" +
                "\t\t\t\"center\":{\n" +
                "\t\t\t\t\"type\":\"geo_point\"\n" +
                "\t\t\t},\n" +
                "\t\t\t\"description\":{\n" +
                "\t\t\t\t\"type\":\"text\"\n" +
                "\t\t\t}\n" +
                "\t\t}\n" +
                "\t}\n" +
                "}";

        client.admin().indices().preparePutMapping(index).setType(type).setSource(dataMapping, XContentType.JSON).execute().actionGet();
    }

    public static void prepareOdbcIndex(){
        String dataMapping = "{\n" +
                "\t\"odbc\" :{\n" +
                "\t\t\"properties\":{\n" +
                "\t\t\t\"odbc_time\":{\n" +
                "\t\t\t\t\"type\":\"date\",\n" +
                "\t\t\t\t\"format\": \"'{ts '''yyyy-MM-dd HH:mm:ss.SSS'''}'\"\n" +
                "\t\t\t},\n" +
                "\t\t\t\"docCount\":{\n" +
                "\t\t\t\t\"type\":\"text\"\n" +
                "\t\t\t}\n" +
                "\t\t}\n" +
                "\t}\n" +
                "}";

        client.admin().indices().preparePutMapping(TEST_INDEX_ODBC).setType("odbc").setSource(dataMapping, XContentType.JSON).execute().actionGet();
    }

	public static SearchDao getSearchDao() {
		return searchDao;
	}

	public static TransportClient getClient() {
		return client;
	}

	protected static TransportAddress getTransportAddress() throws UnknownHostException {
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
		return new TransportAddress(InetAddress.getByName(host), Integer.parseInt(port));
	}
}
