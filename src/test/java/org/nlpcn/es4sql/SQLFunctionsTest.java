package org.nlpcn.es4sql;


import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugin.deletebyquery.DeleteByQueryPlugin;
import org.elasticsearch.plugin.nlpcn.QueryActionElasticExecutor;
import org.elasticsearch.plugin.nlpcn.executors.CSVResult;
import org.elasticsearch.plugin.nlpcn.executors.CSVResultsExtractor;
import org.elasticsearch.plugin.nlpcn.executors.CsvExtractorException;

import org.junit.Assert;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.QueryAction;
import org.nlpcn.es4sql.query.SqlElasticSearchRequestBuilder;
import org.junit.Test;

import java.net.UnknownHostException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;

/**
 * Created by allwefantasy on 8/25/16.
 */
public class SQLFunctionsTest {

    @Test
    public void functionFieldAliasAndGroupByAlias() throws Exception {
        String query = "SELECT " +
                "floor(substring(address,0,3)*20) as key," +
                "sum(age) cvalue FROM " + TestsConstants.TEST_INDEX + "/account where address is not null " +
                "group by key order by cvalue desc limit 10  ";

        CSVResult csvResult = getCsvResult(false, query);
        List<String> headers = csvResult.getHeaders();
        List<String> content = csvResult.getLines();
        Assert.assertEquals(2, headers.size());
        Assert.assertTrue(headers.contains("key"));
        Assert.assertTrue(headers.contains("cvalue"));
        Assert.assertTrue(content.contains("19260.0,167.0"));
    }

    @Test
    public void functionAlias() throws Exception {
        //here is a bug,if only script fields are included,then all fields will return; fix later
        String query = "SELECT " +
                "substring(address,0,3) as key,address from " +
                TestsConstants.TEST_INDEX + "/account where address is not null " +
                "order by address desc limit 10  ";

        CSVResult csvResult = getCsvResult(false, query);
        List<String> headers = csvResult.getHeaders();
        List<String> content = csvResult.getLines();
        Assert.assertTrue(headers.contains("key"));
        Assert.assertTrue(content.contains("863 Wythe Place,863"));
    }

    @Test
    public void normalFieldAlias() throws Exception {

        //here is a bug,csv field with spa
        String query = "SELECT " +
                "address as key,age from " +
                TestsConstants.TEST_INDEX + "/account where address is not null " +
                "limit 10  ";

        CSVResult csvResult = getCsvResult(false, query);
        List<String> headers = csvResult.getHeaders();
        Assert.assertTrue(headers.contains("key"));
    }


    @Test
    public void groupByFieldAlias() throws Exception {

        //here is a bug,csv field with spa
        String query = "SELECT " +
                "age as key,sum(age) from " +
                TestsConstants.TEST_INDEX + "/account where address is not null " +
                " group by key limit 10  ";

        CSVResult csvResult = getCsvResult(false, query);
        List<String> headers = csvResult.getHeaders();
        List<String> contents = csvResult.getLines();
        Assert.assertTrue(headers.contains("key"));
        String[] splits = contents.get(0).split(",");
        Assert.assertTrue(Integer.parseInt(splits[0]) <= Double.parseDouble(splits[1]));
    }

    @Test
    public void concat_ws_field_and_string() throws Exception {

        //here is a bug,csv field with spa
        String query = "SELECT " +
                " concat_ws('-',age,'-'),address from " +
                TestsConstants.TEST_INDEX + "/account " +
                " limit 10  ";

        CSVResult csvResult = getCsvResult(false, query);
        List<String> headers = csvResult.getHeaders();
        List<String> contents = csvResult.getLines();
        String[] splits = contents.get(0).split(",");
		Assert.assertTrue(splits[0].endsWith("--")|| splits[1].endsWith("--"));
	}

	@Test
    public void concat_ws_fields() throws Exception {

        //here is a bug,csv field with spa
        String query = "SELECT " +
                " concat_ws('-',age,address),address from " +
                TestsConstants.TEST_INDEX + "/account " +
                " limit 10  ";

        CSVResult csvResult = getCsvResult(false, query);
        List<String> headers = csvResult.getHeaders();
        List<String> contents = csvResult.getLines();
        Assert.assertTrue(headers.size() == 2);
        Assert.assertTrue(contents.get(0).contains("-"));
    }

    @Test
    public void split_field() throws Exception {

        //here is a bug,csv field with spa
        String query = "SELECT " +
                " split(address,' ')[0],age from " +
                TestsConstants.TEST_INDEX + "/account where address is not null " +
                " limit 10  ";

        CSVResult csvResult = getCsvResult(false, query);
        List<String> headers = csvResult.getHeaders();
        List<String> contents = csvResult.getLines();
        String[] splits = contents.get(0).split(",");
        Assert.assertTrue(headers.size() == 2);
        Assert.assertTrue(Integer.parseInt(splits[0]) > 0);
    }


    private CSVResult getCsvResult(boolean flat, String query) throws SqlParseException, SQLFeatureNotSupportedException, Exception, CsvExtractorException {
        return getCsvResult(flat, query, false, false);
    }

    private CSVResult getCsvResult(boolean flat, String query, boolean includeScore, boolean includeType) throws SqlParseException, SQLFeatureNotSupportedException, Exception, CsvExtractorException {
        SearchDao searchDao = MainTestSuite.getSearchDao() != null ? MainTestSuite.getSearchDao() : getSearchDao();
        QueryAction queryAction = searchDao.explain(query);
        Object execution = QueryActionElasticExecutor.executeAnyAction(searchDao.getClient(), queryAction);
        return new CSVResultsExtractor(includeScore, includeType).extractResults(execution, flat, ",");
    }


    private SearchDao getSearchDao() throws UnknownHostException {
        Settings settings = Settings.builder().put("client.transport.ignore_cluster_name", true).build();
        Client client = TransportClient.builder().addPlugin(DeleteByQueryPlugin.class).settings(settings).
                build().addTransportAddress(MainTestSuite.getTransportAddress());
        return new SearchDao(client);
    }
}
