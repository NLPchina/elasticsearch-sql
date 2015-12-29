package org.nlpcn.es4sql;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.plugin.nlpcn.QueryActionElasticExecutor;
import org.elasticsearch.plugin.nlpcn.executors.CSVResult;
import org.elasticsearch.plugin.nlpcn.executors.CSVResultsExtractor;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
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

    @Test
    public void simpleNumericValueAgg() throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = String.format("select count(*) from %s/dog ",TEST_INDEX);
        CSVResult csvResult = getCsvResult(false, query);

        List<String> headers = csvResult.getHeaders();
        Assert.assertEquals(1, headers.size());
        Assert.assertEquals("COUNT(*)", headers.get(0));


        List<String> lines = csvResult.getLines();
        Assert.assertEquals(1, lines.size());
        Assert.assertEquals("2.0", lines.get(0));

    }
    @Test
    public void simpleNumericValueAggWithAlias() throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = String.format("select avg(age) as myAlias from %s/dog ",TEST_INDEX);
        CSVResult csvResult = getCsvResult(false, query);

        List<String> headers = csvResult.getHeaders();
        Assert.assertEquals(1, headers.size());
        Assert.assertEquals("myAlias", headers.get(0));


        List<String> lines = csvResult.getLines();
        Assert.assertEquals(1, lines.size());
        Assert.assertEquals("3.0", lines.get(0));

    }

    @Test
    public void twoNumericAggWithAlias() throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = String.format("select count(*) as count, avg(age) as myAlias from %s/dog ",TEST_INDEX);
        CSVResult csvResult = getCsvResult(false, query);

        List<String> headers = csvResult.getHeaders();
        Assert.assertEquals(2, headers.size());

        Assert.assertTrue(headers.contains("count"));
        Assert.assertTrue(headers.contains("myAlias"));


        List<String> lines = csvResult.getLines();
        Assert.assertEquals(1, lines.size());
        if(headers.get(0).equals("count")) {
            Assert.assertEquals("2.0,3.0", lines.get(0));
        }
        else {
            Assert.assertEquals("3.0,2.0", lines.get(0));
        }

    }

    @Test
    public void aggAfterTermsGroupBy() throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = String.format("SELECT COUNT(*) FROM %s/account GROUP BY gender",TEST_INDEX);
        CSVResult csvResult = getCsvResult(false, query);
        List<String> headers = csvResult.getHeaders();
        Assert.assertEquals(2, headers.size());
        Assert.assertEquals("gender", headers.get(0));
        Assert.assertEquals("COUNT(*)", headers.get(1));

        List<String> lines = csvResult.getLines();
        Assert.assertEquals(2, lines.size());
        Assert.assertTrue("m,507.0", lines.contains("m,507.0"));
        Assert.assertTrue("f,493.0", lines.contains("f,493.0"));

    }
    @Test
    public void aggAfterTwoTermsGroupBy() throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = String.format("SELECT COUNT(*) FROM %s/account where age in (35,36) GROUP BY gender,age",TEST_INDEX);
        CSVResult csvResult = getCsvResult(false, query);
        List<String> headers = csvResult.getHeaders();
        Assert.assertEquals(3, headers.size());
        Assert.assertEquals("gender", headers.get(0));
        Assert.assertEquals("age", headers.get(1));
        Assert.assertEquals("COUNT(*)", headers.get(2));

        List<String> lines = csvResult.getLines();
        Assert.assertEquals(4, lines.size());
        Assert.assertTrue("m,36,31.0", lines.contains("m,36,31.0"));
        Assert.assertTrue("m,35,28.0", lines.contains("m,36,31.0"));
        Assert.assertTrue("f,36,21.0", lines.contains("f,36,21.0"));
        Assert.assertTrue("f,35,24.0", lines.contains("f,35,24.0"));

    }
    @Test
    public void multipleAggAfterTwoTermsGroupBy() throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = String.format("SELECT COUNT(*) , sum(balance) FROM %s/account where age in (35,36) GROUP BY gender,age",TEST_INDEX);
        CSVResult csvResult = getCsvResult(false, query);
        List<String> headers = csvResult.getHeaders();
        Assert.assertEquals(4, headers.size());
        Assert.assertEquals("gender", headers.get(0));
        Assert.assertEquals("age", headers.get(1));
        Assert.assertTrue(headers.get(2).equals("COUNT(*)") || headers.get(2).equals("SUM(balance)"));
        Assert.assertTrue(headers.get(3).equals("COUNT(*)") || headers.get(3).equals("SUM(balance)"));

        List<String> lines = csvResult.getLines();
        Assert.assertEquals(4, lines.size());
        Assert.assertTrue("m,36,31.0,647425.0", lines.contains("m,36,31.0,647425.0") || lines.contains("m,36,647425.0,31.0"));
        Assert.assertTrue("m,35,28.0,678337.0", lines.contains("m,35,28.0,678337.0") || lines.contains("m,35,678337.0,28.0"));
        Assert.assertTrue("f,36,21.0,505660.0", lines.contains("f,36,21.0,505660.0") || lines.contains("f,36,505660.0,21.0"));
        Assert.assertTrue("f,35,24.0,472771.0", lines.contains("f,35,24.0,472771.0") || lines.contains("f,35,472771.0,24.0"));

    }

    @Test
    public void dateHistogramTest() throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = String.format("select count(*) from %s/online" +
                " group by date_histogram('field'='insert_time','interval'='4d','alias'='days')",TEST_INDEX);
        CSVResult csvResult = getCsvResult(false, query);
        List<String> headers = csvResult.getHeaders();
        Assert.assertEquals(2, headers.size());
        Assert.assertEquals("days", headers.get(0));
        Assert.assertEquals("COUNT(*)", headers.get(1));

        List<String> lines = csvResult.getLines();
        Assert.assertEquals(3, lines.size());
        Assert.assertTrue("2014-08-14 00:00:00,477.0", lines.contains("2014-08-14 00:00:00,477.0"));
        Assert.assertTrue("2014-08-18 00:00:00,5664.0", lines.contains("2014-08-18 00:00:00,5664.0"));
        Assert.assertTrue("2014-08-22 00:00:00,3795.0", lines.contains("2014-08-22 00:00:00,3795.0"));

    }


    /* todo: more tests:
    * multi_numeric extended_stats , stats , percentiles.
    * filter/nested and than metric
    * histogram
    * geo
     */


    private CSVResult getCsvResult(boolean flat, String query) throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        SearchDao searchDao = MainTestSuite.getSearchDao();
        QueryAction queryAction = searchDao.explain(query);
        Object execution =  QueryActionElasticExecutor.executeAnyAction(searchDao.getClient(), queryAction);
        return new CSVResultsExtractor().extractResults(execution, flat, ",");
    }



}
