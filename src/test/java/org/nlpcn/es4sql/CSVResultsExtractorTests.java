package org.nlpcn.es4sql;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.plugin.nlpcn.QueryActionElasticExecutor;
import org.elasticsearch.plugin.nlpcn.executors.CSVResult;
import org.elasticsearch.plugin.nlpcn.executors.CSVResultsExtractor;
import org.elasticsearch.search.SearchHits;
import org.junit.Assert;
import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.QueryAction;
import org.nlpcn.es4sql.query.SqlElasticSearchRequestBuilder;

import java.io.IOException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;

import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX;

/**
 * Created by Eliran on 27/12/2015.
 */
public class CSVResultsExtractorTests {


    @Test
    public void simpleSearchResultNotNestedNotFlatNoAggs() throws SqlParseException, SQLFeatureNotSupportedException, IOException {
       String query = String.format("select name,age from %s/dog order by age",TEST_INDEX);
        CSVResult csvResult = getCsvResult(false, query);

        List<String> headers = csvResult.getHeaders();
        Assert.assertEquals(2, headers.size());
        Assert.assertTrue("name should be on headers", headers.contains("name"));
        Assert.assertTrue("age should be on headers", headers.contains("age"));

        List<String> lines = csvResult.getLines();
        Assert.assertEquals(2, lines.size());
        Assert.assertEquals("rex,2", lines.get(0));
        Assert.assertEquals("snoopy,4", lines.get(1));

    }


    @Test
    public void simpleSearchResultWithNestedNotFlatNoAggs() throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = String.format("select name,house from %s/gotCharacters",TEST_INDEX);
        CSVResult csvResult = getCsvResult(false, query);

        List<String> headers = csvResult.getHeaders();
        Assert.assertEquals(2, headers.size());
        Assert.assertTrue("name should be on headers", headers.contains("name"));
        Assert.assertTrue("house should be on headers", headers.contains("house"));

        List<String> lines = csvResult.getLines();
        Assert.assertEquals(4, lines.size());
        Assert.assertTrue(lines.contains("{firstname=Daenerys, lastname=Targaryen, ofHerName=1},Targaryen") ||
                lines.contains("{firstname=Daenerys, ofHerName=1, lastname=Targaryen},Targaryen") ||
                lines.contains("{lastname=Targaryen, firstname=Daenerys, ofHerName=1},Targaryen") ||
                lines.contains("{lastname=Targaryen, ofHerName=1, firstname=Daenerys},Targaryen") ||
                lines.contains("{ofHerName=1, lastname=Targaryen, firstname=Daenerys},Targaryen") ||
                lines.contains("{ofHerName=1, firstname=Daenerys, lastname=Targaryen},Targaryen")
        );
        //todo: generate all options for rest 3..
    }


    @Test
    public void simpleSearchResultWithNestedOneFieldNotFlatNoAggs() throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = String.format("select name.firstname,house from %s/gotCharacters",TEST_INDEX);
        CSVResult csvResult = getCsvResult(false, query);

        List<String> headers = csvResult.getHeaders();
        Assert.assertEquals(2, headers.size());
        Assert.assertTrue("name should be on headers", headers.contains("name"));
        Assert.assertTrue("house should be on headers", headers.contains("house"));

        List<String> lines = csvResult.getLines();
        Assert.assertEquals(4, lines.size());
        Assert.assertTrue(lines.contains("{firstname=Daenerys},Targaryen"));
        Assert.assertTrue(lines.contains("{firstname=Eddard},Stark"));
        Assert.assertTrue(lines.contains("{firstname=Brandon},Stark"));
        Assert.assertTrue(lines.contains("{firstname=Jaime},Lannister"));

    }

    @Test
    public void simpleSearchResultWithNestedTwoFieldsFromSameNestedNotFlatNoAggs() throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = String.format("select name.firstname,name.lastname,house from %s/gotCharacters", TEST_INDEX);
        CSVResult csvResult = getCsvResult(false, query);

        List<String> headers = csvResult.getHeaders();
        Assert.assertEquals(2, headers.size());
        Assert.assertTrue("name should be on headers", headers.contains("name"));
        Assert.assertTrue("house should be on headers", headers.contains("house"));

        List<String> lines = csvResult.getLines();
        Assert.assertEquals(4, lines.size());
        Assert.assertTrue(lines.contains("{firstname=Daenerys, lastname=Targaryen},Targaryen") ||
                lines.contains("{lastname=Targaryen, firstname=Daenerys},Targaryen"));
        Assert.assertTrue(lines.contains("{firstname=Eddard, lastname=Stark},Stark") ||
                lines.contains("{lastname=Stark, firstname=Eddard},Stark"));
        Assert.assertTrue(lines.contains("{firstname=Brandon, lastname=Stark},Stark") ||
                lines.contains("{lastname=Stark, firstname=Brandon},Stark"));
        Assert.assertTrue(lines.contains("{firstname=Jaime, lastname=Lannister},Lannister") ||
                lines.contains("{lastname=Lannister, firstname=Jaime},Lannister") );

    }

    @Test
    public void simpleSearchResultWithNestedWithFlatNoAggs() throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = String.format("select name.firstname,house from %s/gotCharacters",TEST_INDEX);
        CSVResult csvResult = getCsvResult(true, query);

        List<String> headers = csvResult.getHeaders();
        Assert.assertEquals(2, headers.size());
        Assert.assertTrue("name.firstname should be on headers", headers.contains("name.firstname"));
        Assert.assertTrue("house should be on headers", headers.contains("house"));

        List<String> lines = csvResult.getLines();
        Assert.assertEquals(4, lines.size());
        Assert.assertTrue(lines.contains("Daenerys,Targaryen"));
        Assert.assertTrue(lines.contains("Eddard,Stark"));
        Assert.assertTrue(lines.contains("Brandon,Stark"));
        Assert.assertTrue(lines.contains("Jaime,Lannister"));

    }
    @Test
    public void joinSearchResultNotNestedNotFlatNoAggs() throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = String.format("select c.gender , h.name,h.words from %s/gotCharacters c " +
                "JOIN %s/gotHouses h " +
                "on h.name = c.house ",TEST_INDEX,TEST_INDEX);
        CSVResult csvResult = getCsvResult(false, query);

        List<String> headers = csvResult.getHeaders();
        Assert.assertEquals(3, headers.size());
        Assert.assertTrue("c.gender should be on headers", headers.contains("c.gender"));
        Assert.assertTrue("h.words should be on headers", headers.contains("h.words"));
        Assert.assertTrue("h.words should be on headers", headers.contains("h.words"));

        List<String> lines = csvResult.getLines();
        Assert.assertEquals(4, lines.size());
        Assert.assertTrue(lines.contains("F,Targaryen,fireAndBlood") ||
                          lines.contains("F,fireAndBlood,Targaryen") ||
                          lines.contains("Targaryen,fireAndBlood,F") ||
                          lines.contains("Targaryen,F,fireAndBlood") ||
                          lines.contains("fireAndBlood,Targaryen,F") ||
                          lines.contains("fireAndBlood,F,Targaryen")

        );

    }
    private CSVResult getCsvResult(boolean flat, String query) throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        SearchDao searchDao = MainTestSuite.getSearchDao();
        QueryAction queryAction = searchDao.explain(query);
        SearchHits searchHits = (SearchHits) QueryActionElasticExecutor.executeAnyAction(searchDao.getClient(), queryAction);
        return CSVResultsExtractor.extractResults(searchHits, flat, ",");
    }



}
