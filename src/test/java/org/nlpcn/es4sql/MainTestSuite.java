package org.nlpcn.es4sql;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.Version;
import co.elastic.clients.transport.rest_client.RestClientOptions;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicNameValuePair;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.plugin.nlpcn.client.ElasticsearchRestClient;
import org.elasticsearch.xcontent.XContentType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.io.FileInputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.function.Consumer;

import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_ACCOUNT_TEMP;
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

	private static ElasticsearchRestClient client;
	private static SearchDao searchDao;

	@BeforeClass
	public static void setUp() throws Exception {
		client = createElasticsearchClient();

        NodesInfoResponse nodeInfos = client.admin().cluster().prepareNodesInfo().clear().setHttp(true).setOs(true).setProcess(true).setThreadPool(true).setIndices(true).get();
		String clusterName = nodeInfos.getClusterName().value();
		System.out.println(String.format("Found cluster... cluster name: %s", clusterName));

        client.admin().cluster().prepareUpdateSettings(TimeValue.ONE_MINUTE, TimeValue.ONE_MINUTE).setTransientSettings(ImmutableMap.of(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), false)).get();

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

        createTestIndex(TEST_INDEX_ACCOUNT_TEMP);
        loadBulk("src/test/resources/accounts_temp.json", TEST_INDEX_ACCOUNT_TEMP);
        client.admin().indices().prepareRefresh(TEST_INDEX_ACCOUNT_TEMP).get();

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
        if(client.admin().cluster().prepareState(TimeValue.ONE_MINUTE).execute().actionGet().getState().getMetadata().hasIndex(index)){
            client.admin().indices().prepareDelete(index).get();
        }
    }

    private static void prepareGameOfThronesIndex() {
        String dataMapping = "{ " +
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
                "} }";
        client.admin().indices().preparePutMapping(TEST_INDEX_GAME_OF_THRONES).setSource(dataMapping, XContentType.JSON).execute().actionGet();
    }

    private static void prepareDogsIndex() {
        String dataMapping = "{" +
                " \"properties\": {\n" +
                "          \"dog_name\": {\n" +
                "            \"type\": \"text\",\n" +
                "            \"fielddata\": true\n" +
                "          }"+
                "       }"+
                "   }";
        client.admin().indices().preparePutMapping(TEST_INDEX_DOG).setSource(dataMapping, XContentType.JSON).execute().actionGet();
    }

    private static void prepareAccountsIndex() {
        String dataMapping = "{" +
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
                "   }";
        client.admin().indices().preparePutMapping(TEST_INDEX_ACCOUNT).setSource(dataMapping, XContentType.JSON).execute().actionGet();
    }

    private static void preparePhrasesIndex() {
        String dataMapping = "{" +
                " \"properties\": {\n" +
                "          \"phrase\": {\n" +
                "            \"type\": \"text\",\n" +
                "            \"store\": true\n" +
                "          }" +
                "       }"+
                "   }";
        client.admin().indices().preparePutMapping(TEST_INDEX_PHRASE).setSource(dataMapping, XContentType.JSON).execute().actionGet();
    }

    private static void prepareNestedTypeIndex() {

            String dataMapping = "{\n" +
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
                    "    }";

            client.admin().indices().preparePutMapping(TEST_INDEX_NESTED_TYPE).setSource(dataMapping, XContentType.JSON).execute().actionGet();
    }

    private static void prepareJoinTypeIndex() {
        String dataMapping = "{\n" +
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
                "  }\n";
        client.admin().indices().preparePutMapping(TEST_INDEX_JOIN_TYPE).setSource(dataMapping, XContentType.JSON).execute().actionGet();
    }

    @AfterClass
	public static void tearDown() {
		System.out.println("teardown process...");

        client.admin().indices().prepareDelete(TEST_INDEX + "*").get();

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

        DeleteByQueryRequestBuilder deleteQueryBuilder = new DeleteByQueryRequestBuilder(client);
        deleteQueryBuilder.request().indices(indexName);
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
                "\t}\n";

        client.admin().indices().preparePutMapping(index).setSource(dataMapping, XContentType.JSON).execute().actionGet();
    }

    public static void prepareOdbcIndex(){
        String dataMapping = "{\n" +
                "\t\t\"properties\":{\n" +
                "\t\t\t\"odbc_time\":{\n" +
                "\t\t\t\t\"type\":\"date\",\n" +
                "\t\t\t\t\"format\": \"'{ts '''yyyy-MM-dd HH:mm:ss.SSS'''}'\"\n" +
                "\t\t\t},\n" +
                "\t\t\t\"docCount\":{\n" +
                "\t\t\t\t\"type\":\"text\"\n" +
                "\t\t\t}\n" +
                "\t\t}\n" +
                "\t}\n";

        client.admin().indices().preparePutMapping(TEST_INDEX_ODBC).setSource(dataMapping, XContentType.JSON).execute().actionGet();
    }

	public static SearchDao getSearchDao() {
		return searchDao;
	}

    public static Client getClient() {
        return client;
    }

    public static ElasticsearchRestClient createElasticsearchClient() throws UnknownHostException {
        return new ElasticsearchRestClient(new ElasticsearchClient(getElasticsearchTransport(getRestClient())));
    }

    private static RestClient getRestClient() throws UnknownHostException {
        InetSocketAddress address = getTransportAddress().address();
        String hostPort = String.format("http://%s:%s", address.getHostString(), address.getPort());

        RestClientBuilder builder = RestClient.builder(HttpHost.create(hostPort));
        builder.setHttpClientConfigCallback(clientBuilder -> {
            RequestConfig.Builder requestConfigBuilder = RequestConfig.custom();
            requestConfigBuilder.setConnectTimeout(10 * 1000);

            int socketTimeout = 90 * 1000;
            requestConfigBuilder.setSocketTimeout(socketTimeout);
            requestConfigBuilder.setConnectionRequestTimeout(socketTimeout);
            clientBuilder.setDefaultRequestConfig(requestConfigBuilder.build());

            return clientBuilder;
        });
        return builder.build();
    }

    private static TransportAddress getTransportAddress() throws UnknownHostException {
        String host = System.getenv("ES_TEST_HOST");
        String port = System.getenv("ES_TEST_PORT");

        if (host == null) {
            host = "localhost";
            System.out.println("ES_TEST_HOST enviroment variable does not exist. choose default 'localhost'");
        }

        if (port == null) {
            port = "9200";
            System.out.println("ES_TEST_PORT enviroment variable does not exist. choose default '9200'");
        }

        System.out.println(String.format("Connection details: host: %s. port:%s.", host, port));
        return new TransportAddress(InetAddress.getByName(host), Integer.parseInt(port));
    }

    private static ElasticsearchTransport getElasticsearchTransport(RestClient restClient) {
        RestClientOptions.Builder transportOptionsBuilder = new RestClientOptions(RequestOptions.DEFAULT, true).toBuilder();

        ContentType jsonContentType = Version.VERSION == null ? ContentType.APPLICATION_JSON
                : ContentType.create("application/vnd.elasticsearch+json",
                new BasicNameValuePair("compatible-with", String.valueOf(Version.VERSION.major())));

        Consumer<String> setHeaderIfNotPresent = header -> {
            if (transportOptionsBuilder.build().headers().stream().noneMatch((h) -> h.getKey().equalsIgnoreCase(header))) {
                transportOptionsBuilder.addHeader(header, jsonContentType.toString());
            }
        };

        setHeaderIfNotPresent.accept("Content-Type");
        setHeaderIfNotPresent.accept("Accept");

        RestClientOptions transportOptionsWithHeader = transportOptionsBuilder.build();
        return new RestClientTransport(restClient, new JacksonJsonpMapper(), transportOptionsWithHeader);
    }
}
