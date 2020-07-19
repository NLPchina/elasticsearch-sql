package org.nlpcn.es4sql;

import org.elasticsearch.plugin.nlpcn.QueryActionElasticExecutor;
import org.elasticsearch.plugin.nlpcn.executors.CSVResult;
import org.elasticsearch.plugin.nlpcn.executors.CSVResultsExtractor;
import org.junit.Assert;
import org.junit.Test;
import org.nlpcn.es4sql.query.QueryAction;

import java.util.List;

import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_DOG;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_GAME_OF_THRONES;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_ONLINE;

/**
 * Created by Eliran on 27/12/2015.
 */
public class CSVResultsExtractorTests {


    @Test
    public void simpleSearchResultNotNestedNotFlatNoAggs() throws Exception {
       String query = String.format("select dog_name,age from %s/dog order by age",TEST_INDEX_DOG);
        CSVResult csvResult = getCsvResult(false, query);

        List<String> headers = csvResult.getHeaders();
        Assert.assertEquals(2, headers.size());
        Assert.assertTrue("name should be on headers", headers.contains("dog_name"));
        Assert.assertTrue("age should be on headers", headers.contains("age"));

        List<String> lines = csvResult.getLines();
        Assert.assertEquals(2, lines.size());
        Assert.assertTrue("rex,2".equals(lines.get(0)) || "2,rex".equals(lines.get(0)) );
        Assert.assertTrue("snoopy,4".equals(lines.get(1)) || "4,snoopy".equals(lines.get(1)) );

    }


    @Test
    public void simpleSearchResultWithNestedNotFlatNoAggs() throws Exception {
        String query = String.format("select name,house from %s/gotCharacters",TEST_INDEX_GAME_OF_THRONES);
        CSVResult csvResult = getCsvResult(false, query);

        List<String> headers = csvResult.getHeaders();
        Assert.assertEquals(2, headers.size());
        Assert.assertTrue("name should be on headers", headers.contains("name"));
        Assert.assertTrue("house should be on headers", headers.contains("house"));

        List<String> lines = csvResult.getLines();
        Assert.assertEquals(7, lines.size());
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
    public void simpleSearchResultWithNestedOneFieldNotFlatNoAggs() throws Exception {
        String query = String.format("select name.firstname,house from %s/gotCharacters",TEST_INDEX_GAME_OF_THRONES);
        CSVResult csvResult = getCsvResult(false, query);

        List<String> headers = csvResult.getHeaders();
        Assert.assertEquals(2, headers.size());
        Assert.assertTrue("name should be on headers", headers.contains("name"));
        Assert.assertTrue("house should be on headers", headers.contains("house"));

        List<String> lines = csvResult.getLines();
        Assert.assertEquals(7, lines.size());
        Assert.assertTrue(lines.contains("Targaryen,{firstname=Daenerys}"));
        Assert.assertTrue(lines.contains("Stark,{firstname=Eddard}"));
        Assert.assertTrue(lines.contains("Stark,{firstname=Brandon}"));
        Assert.assertTrue(lines.contains("Lannister,{firstname=Jaime}"));

    }

    @Test
    public void simpleSearchResultWithNestedTwoFieldsFromSameNestedNotFlatNoAggs() throws Exception {
        String query = String.format("select name.firstname,name.lastname,house from %s/gotCharacters", TEST_INDEX_GAME_OF_THRONES);
        CSVResult csvResult = getCsvResult(false, query);

        List<String> headers = csvResult.getHeaders();
        Assert.assertEquals(2, headers.size());
        Assert.assertTrue("name should be on headers", headers.contains("name"));
        Assert.assertTrue("house should be on headers", headers.contains("house"));

        List<String> lines = csvResult.getLines();
        Assert.assertEquals(7, lines.size());
        Assert.assertTrue(lines.contains("Targaryen,{firstname=Daenerys, lastname=Targaryen}") ||
                lines.contains("Targaryen,{lastname=Targaryen, firstname=Daenerys}"));
        Assert.assertTrue(lines.contains("Stark,{firstname=Eddard, lastname=Stark}") ||
                lines.contains("Stark,{lastname=Stark, firstname=Eddard}"));
        Assert.assertTrue(lines.contains("Stark,{firstname=Brandon, lastname=Stark}") ||
                lines.contains("Stark,{lastname=Stark, firstname=Brandon}"));
        Assert.assertTrue(lines.contains("Lannister,{firstname=Jaime, lastname=Lannister}") ||
                lines.contains("Lannister,{lastname=Lannister, firstname=Jaime}") );

    }

    @Test
    public void simpleSearchResultWithNestedWithFlatNoAggs() throws Exception {
        String query = String.format("select name.firstname,house from %s/gotCharacters",TEST_INDEX_GAME_OF_THRONES);
        CSVResult csvResult = getCsvResult(true, query);

        List<String> headers = csvResult.getHeaders();
        Assert.assertEquals(2, headers.size());
        Assert.assertTrue("name.firstname should be on headers", headers.contains("name.firstname"));
        Assert.assertTrue("house should be on headers", headers.contains("house"));

        List<String> lines = csvResult.getLines();
        Assert.assertEquals(7, lines.size());
        Assert.assertTrue(lines.contains("Daenerys,Targaryen"));
        Assert.assertTrue(lines.contains("Eddard,Stark"));
        Assert.assertTrue(lines.contains("Brandon,Stark"));
        Assert.assertTrue(lines.contains("Jaime,Lannister"));

    }
    @Test
    public void joinSearchResultNotNestedNotFlatNoAggs() throws Exception {
        String query = String.format("select c.gender , h.hname,h.words from %s/gotCharacters c " +
                "JOIN %s/gotCharacters h " +
                "on h.hname = c.house ",TEST_INDEX_GAME_OF_THRONES,TEST_INDEX_GAME_OF_THRONES);
        CSVResult csvResult = getCsvResult(false, query);

        List<String> headers = csvResult.getHeaders();
        Assert.assertEquals(3, headers.size());
        Assert.assertTrue("c.gender should be on headers", headers.contains("c.gender"));
        Assert.assertTrue("h.hname should be on headers", headers.contains("h.hname"));
        Assert.assertTrue("h.words should be on headers", headers.contains("h.words"));

        List<String> lines = csvResult.getLines();
        Assert.assertEquals(16, lines.size());
        Assert.assertTrue(lines.contains("F,Targaryen,fireAndBlood") ||
                          lines.contains("F,fireAndBlood,Targaryen") ||
                          lines.contains("Targaryen,fireAndBlood,F") ||
                          lines.contains("Targaryen,F,fireAndBlood") ||
                          lines.contains("fireAndBlood,Targaryen,F") ||
                          lines.contains("fireAndBlood,F,Targaryen")

        );

    }

    @Test
    public void simpleNumericValueAgg() throws Exception {
        String query = String.format("select count(*) from %s/dog ",TEST_INDEX_DOG);
        CSVResult csvResult = getCsvResult(false, query);

        List<String> headers = csvResult.getHeaders();
        Assert.assertEquals(1, headers.size());
        Assert.assertEquals("COUNT(*)", headers.get(0));


        List<String> lines = csvResult.getLines();
        Assert.assertEquals(1, lines.size());
        Assert.assertEquals("2.0", lines.get(0));

    }
    @Test
    public void simpleNumericValueAggWithAlias() throws Exception {
        String query = String.format("select avg(age) as myAlias from %s/dog ",TEST_INDEX_DOG);
        CSVResult csvResult = getCsvResult(false, query);

        List<String> headers = csvResult.getHeaders();
        Assert.assertEquals(1, headers.size());
        Assert.assertEquals("myAlias", headers.get(0));


        List<String> lines = csvResult.getLines();
        Assert.assertEquals(1, lines.size());
        Assert.assertEquals("3.0", lines.get(0));

    }

    @Test
    public void twoNumericAggWithAlias() throws Exception {
        String query = String.format("select count(*) as count, avg(age) as myAlias from %s/dog ",TEST_INDEX_DOG);
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
    public void aggAfterTermsGroupBy() throws Exception {
        String query = String.format("SELECT COUNT(*) FROM %s/account GROUP BY gender",TEST_INDEX_ACCOUNT);
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
    public void aggAfterTwoTermsGroupBy() throws Exception {
        String query = String.format("SELECT COUNT(*) FROM %s/account where age in (35,36) GROUP BY gender,age",TEST_INDEX_ACCOUNT);
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
    public void multipleAggAfterTwoTermsGroupBy() throws Exception {
        String query = String.format("SELECT COUNT(*) , sum(balance) FROM %s/account where age in (35,36) GROUP BY gender,age",TEST_INDEX_ACCOUNT);
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
    public void dateHistogramTest() throws Exception {
        String query = String.format("select count(*) from %s/online" +
                " group by date_histogram('field'='insert_time','interval'='4d','alias'='days')",TEST_INDEX_ONLINE);
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

    @Test
    public void statsAggregationTest() throws Exception {
        String query = String.format("SELECT STATS(age) FROM %s/account", TEST_INDEX_ACCOUNT);
        CSVResult csvResult = getCsvResult(false, query);
        List<String> headers = csvResult.getHeaders();
        Assert.assertEquals(5, headers.size());
        Assert.assertEquals("STATS(age).count", headers.get(0));
        Assert.assertEquals("STATS(age).sum", headers.get(1));
        Assert.assertEquals("STATS(age).avg", headers.get(2));
        Assert.assertEquals("STATS(age).min", headers.get(3));
        Assert.assertEquals("STATS(age).max", headers.get(4));

        List<String> lines = csvResult.getLines();
        Assert.assertEquals(1, lines.size());
        Assert.assertEquals("1000,30171.0,30.171,20.0,40.0", lines.get(0));

    }

    @Test
    public void extendedStatsAggregationTest() throws Exception {
        String query = String.format("SELECT EXTENDED_STATS(age) FROM %s/account", TEST_INDEX_ACCOUNT);
        CSVResult csvResult = getCsvResult(false, query);
        List<String> headers = csvResult.getHeaders();
        Assert.assertEquals(8, headers.size());
        Assert.assertEquals("EXTENDED_STATS(age).count", headers.get(0));
        Assert.assertEquals("EXTENDED_STATS(age).sum", headers.get(1));
        Assert.assertEquals("EXTENDED_STATS(age).avg", headers.get(2));
        Assert.assertEquals("EXTENDED_STATS(age).min", headers.get(3));
        Assert.assertEquals("EXTENDED_STATS(age).max", headers.get(4));
        Assert.assertEquals("EXTENDED_STATS(age).sumOfSquares", headers.get(5));
        Assert.assertEquals("EXTENDED_STATS(age).variance", headers.get(6));
        Assert.assertEquals("EXTENDED_STATS(age).stdDeviation", headers.get(7));

        List<String> lines = csvResult.getLines();
        Assert.assertEquals(1, lines.size());
        String line = lines.get(0);
        Assert.assertTrue(line.startsWith("1000,30171.0,30.171,20.0,40.0,946393.0"));
        Assert.assertTrue(line.contains(",6.008"));
        Assert.assertTrue(line.contains(",36.103"));
    }

    @Test
    public void percentileAggregationTest() throws Exception {
        String query = String.format("select percentiles(age) as per from %s/account where age > 31", TEST_INDEX_ACCOUNT);
        CSVResult csvResult = getCsvResult(false, query);
        List<String> headers = csvResult.getHeaders();
        Assert.assertEquals(7, headers.size());
        Assert.assertEquals("per.1.0", headers.get(0));
        Assert.assertEquals("per.5.0", headers.get(1));
        Assert.assertEquals("per.25.0", headers.get(2));
        Assert.assertEquals("per.50.0", headers.get(3));
        Assert.assertEquals("per.75.0", headers.get(4));
        Assert.assertEquals("per.95.0", headers.get(5));
        Assert.assertEquals("per.99.0", headers.get(6));


        List<String> lines = csvResult.getLines();
        Assert.assertEquals(1, lines.size());
        Assert.assertEquals("32.0,32.0,34.0,36.0,38.0,40.0,40.0", lines.get(0));
    }

    @Test
    public void includeTypeAndNotScore() throws Exception {
        String query = String.format("select age , firstname from %s/account where age > 31 limit 2", TEST_INDEX_ACCOUNT);
        CSVResult csvResult = getCsvResult(false, query,false,true);
        List<String> headers = csvResult.getHeaders();
        Assert.assertEquals(3,headers.size());
        Assert.assertTrue(headers.contains("age"));
        Assert.assertTrue(headers.contains("firstname"));
        Assert.assertTrue(headers.contains("_type"));
        List<String> lines = csvResult.getLines();
        Assert.assertTrue(lines.get(0).contains(",account") || lines.get(0).contains("account,"));
        Assert.assertTrue(lines.get(1).contains(",account")|| lines.get(1).contains("account,"));
    }

    @Test
    public void includeScoreAndNotType() throws Exception {
        String query = String.format("select age , firstname from %s/account where age > 31 order by _score desc limit 2 ", TEST_INDEX_ACCOUNT);
        CSVResult csvResult = getCsvResult(false, query,true,false);
        List<String> headers = csvResult.getHeaders();
        Assert.assertEquals(3, headers.size());
        Assert.assertTrue(headers.contains("age"));
        Assert.assertTrue(headers.contains("firstname"));
        Assert.assertTrue(headers.contains("_score"));
        List<String> lines = csvResult.getLines();
        Assert.assertTrue(lines.get(0).contains("1.0"));
        Assert.assertTrue(lines.get(1).contains("1.0"));
    }

    @Test
    public void includeScoreAndType() throws Exception {
        String query = String.format("select age , firstname from %s/account where age > 31 order by _score desc limit 2 ", TEST_INDEX_ACCOUNT);
        CSVResult csvResult = getCsvResult(false, query,true,true);
        List<String> headers = csvResult.getHeaders();
        Assert.assertEquals(4, headers.size());
        Assert.assertTrue(headers.contains("age"));
        Assert.assertTrue(headers.contains("firstname"));
        Assert.assertTrue(headers.contains("_score"));
        Assert.assertTrue(headers.contains("_type"));
        List<String> lines = csvResult.getLines();
        String firstLine = lines.get(0);
        System.out.println(firstLine);
        Assert.assertTrue(firstLine.contains("account,1.0") || firstLine.contains("1.0,account"));
        Assert.assertTrue(lines.get(1).contains("account,1.0") || lines.get(1).contains("1.0,account"));
    }

    /* todo: more tests:
    * filter/nested and than metric
    * histogram
    * geo
     */

    @Test
    public void scriptedField() throws Exception {
        String query = String.format("select age+1 as agePlusOne ,age , firstname from %s/account where age =  31 limit 1", TEST_INDEX_ACCOUNT);
        CSVResult csvResult = getCsvResult(false, query,false,false);
        List<String> headers = csvResult.getHeaders();
        Assert.assertEquals(3,headers.size());
        Assert.assertTrue(headers.contains("agePlusOne"));
        Assert.assertTrue(headers.contains("age"));
        Assert.assertTrue(headers.contains("firstname"));
        List<String> lines = csvResult.getLines();
        System.out.println("lines.get(0) = " + lines.get(0));
        Assert.assertTrue(lines.get(0).contains("32,31") || lines.get(0).contains("32.0,31.0") || lines.get(0).contains("31,32")|| lines.get(0).contains("31.0,32.0"));
    }


    @Test
    public void twoCharsSeperator() throws Exception {
        String query = String.format("select dog_name,age from %s/dog order by age",TEST_INDEX_DOG);
        CSVResult csvResult = getCsvResult(false, query,false,false,"||");

        List<String> headers = csvResult.getHeaders();
        Assert.assertEquals(2, headers.size());
        Assert.assertTrue("name should be on headers", headers.contains("dog_name"));
        Assert.assertTrue("age should be on headers", headers.contains("age"));

        List<String> lines = csvResult.getLines();
        Assert.assertEquals(2, lines.size());
        Assert.assertTrue("rex||2".equals(lines.get(0)) || "2||rex".equals(lines.get(0)) );
        Assert.assertTrue("snoopy||4".equals(lines.get(1)) || "4||snoopy".equals(lines.get(1)) );

    }


    @Test
    public void includeIdAndNotTypeOrScore() throws Exception {
        String query = String.format("select age , firstname from %s/account where lastname = 'Marquez' ", TEST_INDEX_ACCOUNT);
        CSVResult csvResult = getCsvResult(false, query,false,false,true);
        List<String> headers = csvResult.getHeaders();
        Assert.assertEquals(3,headers.size());
        Assert.assertTrue(headers.contains("age"));
        Assert.assertTrue(headers.contains("firstname"));
        Assert.assertTrue(headers.contains("_id"));
        List<String> lines = csvResult.getLines();
        Assert.assertTrue(lines.get(0).contains(",437") || lines.get(0).contains("437,"));
    }

    @Test
    public void includeIdAndTypeButNoScore() throws Exception {
        String query = String.format("select age , firstname from %s/account where lastname = 'Marquez' ", TEST_INDEX_ACCOUNT);
        CSVResult csvResult = getCsvResult(false, query,false,true,true);
        List<String> headers = csvResult.getHeaders();
        Assert.assertEquals(4, headers.size());
        Assert.assertTrue(headers.contains("age"));
        Assert.assertTrue(headers.contains("firstname"));
        Assert.assertTrue(headers.contains("_id"));
        Assert.assertTrue(headers.contains("_type"));
        List<String> lines = csvResult.getLines();
        System.out.println(lines.get(0));
        Assert.assertTrue(lines.get(0).contains("account,437") || lines.get(0).contains("437,account"));
    }


    private CSVResult getCsvResult(boolean flat, String query) throws Exception {
        return getCsvResult(flat,query,false,false);
    }


    private CSVResult getCsvResult(boolean flat, String query,boolean includeScore , boolean includeType,boolean includeId) throws Exception {
        return getCsvResult(flat,query,includeScore,includeType,includeId,",");
    }

    private CSVResult getCsvResult(boolean flat, String query,boolean includeScore , boolean includeType) throws Exception {
        return getCsvResult(flat,query,includeScore,includeType,false,",");
    }

    private CSVResult getCsvResult(boolean flat, String query,boolean includeScore , boolean includeType,String seperator) throws Exception {
        return getCsvResult(flat,query,includeScore,includeType,false,seperator);
    }

    private CSVResult getCsvResult(boolean flat, String query,boolean includeScore , boolean includeType,boolean includeId,String seperator) throws Exception {
        SearchDao searchDao = MainTestSuite.getSearchDao();
        QueryAction queryAction = searchDao.explain(query);
        Object execution =  QueryActionElasticExecutor.executeAnyAction(searchDao.getClient(), queryAction);
        return new CSVResultsExtractor(includeScore,includeType, includeId,false, queryAction).extractResults(execution, flat, seperator, false);
    }

}
