package org.nlpcn.es4sql;

import org.elasticsearch.plugin.nlpcn.ElasticHitsExecutor;
import org.elasticsearch.plugin.nlpcn.MultiRequestExecutorFactory;
import org.elasticsearch.search.SearchHit;
import org.junit.Assert;
import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.SqlElasticRequestBuilder;

import java.io.IOException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_DOG;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_GAME_OF_THRONES;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_SYSTEM;

/**
 * Created by Eliran on 21/8/2016.
 */
public class MultiQueryTests {

    private static String MINUS_SCROLL_DEFAULT_HINT = " /*! MINUS_SCROLL_FETCH_AND_RESULT_LIMITS(1000,50,100) */ ";
    private static String MINUS_TERMS_OPTIMIZATION_HINT = " /*! MINUS_USE_TERMS_OPTIMIZATION(true)*/ ";
    @Test
    public void unionAllSameRequestOnlyOneRecordTwice() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        String query = String.format("SELECT firstname FROM %s/account WHERE firstname = 'Amber' limit 1 union all SELECT firstname FROM %s/account WHERE firstname = 'Amber'",TEST_INDEX_ACCOUNT,TEST_INDEX_ACCOUNT);
        SearchHit[] searchHits = executeAndGetHits(query);
        Assert.assertEquals(2,searchHits.length);
        for(SearchHit hit : searchHits) {
            Object firstname = hit.getSourceAsMap().get("firstname");
            Assert.assertEquals("Amber",firstname);
        }
    }

    @Test
    public void unionAllOnlyOneRecordEachWithAlias() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        String query = String.format("SELECT firstname FROM %s/account WHERE firstname = 'Amber' " +
                "union all " +
                "SELECT dog_name as firstname FROM %s/dog WHERE dog_name = 'rex'",TEST_INDEX_ACCOUNT,TEST_INDEX_DOG);
        SearchHit[] searchHits = executeAndGetHits(query);
        Assert.assertEquals(2,searchHits.length);
        Set<String> names = new HashSet<>();
        for(SearchHit hit : searchHits) {
            Object firstname = hit.getSourceAsMap().get("firstname");
            names.add(firstname.toString());
        }
        Assert.assertTrue("names should contain Amber",names.contains("Amber"));
        Assert.assertTrue("names should contain rex",names.contains("rex"));
    }

    @Test
    public void unionAllOnlyOneRecordEachWithComplexAlias() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        String query = String.format("SELECT firstname FROM %s/account WHERE firstname = 'Amber' " +
                "union all " +
                "SELECT name.firstname as firstname FROM %s/gotCharacters WHERE name.firstname = 'Daenerys'",TEST_INDEX_ACCOUNT,TEST_INDEX_GAME_OF_THRONES);
        SearchHit[] searchHits = executeAndGetHits(query);
        Assert.assertEquals(2,searchHits.length);
        Set<String> names = new HashSet<>();
        for(SearchHit hit : searchHits) {
            Object firstname = hit.getSourceAsMap().get("firstname");
            names.add(firstname.toString());
        }
        Assert.assertTrue("names should contain Amber",names.contains("Amber"));
        Assert.assertTrue("names should contain Daenerys",names.contains("Daenerys"));
    }

    @Test
    public void minusAMinusANoAlias() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        innerMinusAMinusANoAlias("");
    }


    @Test
    public void minusAMinusANoAliasWithScrolling() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        innerMinusAMinusANoAlias(MINUS_SCROLL_DEFAULT_HINT);
    }

    @Test
    public void minusAMinusANoAliasWithScrollingAndTerms() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        innerMinusAMinusANoAlias(MINUS_SCROLL_DEFAULT_HINT + MINUS_TERMS_OPTIMIZATION_HINT);
    }


    private void innerMinusAMinusANoAlias(String hint) throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = String.format("SELECT "+hint+" pk FROM %s/systems WHERE system_name = 'A' " +
                "minus " +
                "SELECT pk FROM %s/systems WHERE system_name = 'A' ",TEST_INDEX_SYSTEM,TEST_INDEX_SYSTEM);
        SearchHit[] searchHits = executeAndGetHits(query);
        Assert.assertEquals("no hits should be return", 0, searchHits.length);
    }

    @Test
    public void minusAMinusBNoAlias() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        innerMinus_AMinusBNoAlias("");
    }

    @Test
    public void minusAMinusBNoAliasWithScrolling() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        innerMinus_AMinusBNoAlias(MINUS_SCROLL_DEFAULT_HINT);
    }

    @Test
    public void minusAMinusBNoAliasWithScrollingAndTerms () throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        innerMinus_AMinusBNoAlias(MINUS_SCROLL_DEFAULT_HINT + MINUS_TERMS_OPTIMIZATION_HINT);
    }


    private void innerMinus_AMinusBNoAlias(String hint) throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = String.format("SELECT "+hint+" pk FROM %s/systems WHERE system_name = 'A' " +
                "minus " +
                "SELECT pk FROM %s/systems WHERE system_name = 'B' ",TEST_INDEX_SYSTEM,TEST_INDEX_SYSTEM);
        SearchHit[] searchHits = executeAndGetHits(query);
        Assert.assertEquals("not exactly one hit returned", 1, searchHits.length);
        Map<String, Object> sourceAsMap = searchHits[0].getSourceAsMap();
        Assert.assertEquals("source map not contained exactly one field",1,sourceAsMap.size());
        Assert.assertTrue("source map should contain pk",sourceAsMap.containsKey("pk"));
        Assert.assertEquals(3, sourceAsMap.get("pk"));
    }


    @Test
    public void minusCMinusDTwoFieldsNoAlias() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
            innerMinus_CMinusDTwoFieldsNoAlias("");
    }


    @Test
    public void minusCMinusDTwoFieldsNoAliasWithScrolling() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        innerMinus_CMinusDTwoFieldsNoAlias(MINUS_SCROLL_DEFAULT_HINT);
    }


    private void innerMinus_CMinusDTwoFieldsNoAlias(String hint) throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = String.format("SELECT " + hint + " pk , letter  FROM %s/systems WHERE system_name = 'C' " +
                "minus " +
                "SELECT pk , letter FROM %s/systems WHERE system_name = 'D' ",TEST_INDEX_SYSTEM,TEST_INDEX_SYSTEM);
        SearchHit[] searchHits = executeAndGetHits(query);
        Assert.assertEquals("not exactly one hit returned", 1, searchHits.length);
        Map<String, Object> sourceAsMap = searchHits[0].getSourceAsMap();
        Assert.assertEquals("source map not contained exactly two fields",2,sourceAsMap.size());
        Assert.assertTrue("source map should contain pk",sourceAsMap.containsKey("pk"));
        Assert.assertTrue("source map should contain letter",sourceAsMap.containsKey("letter"));
        Assert.assertEquals(1,sourceAsMap.get("pk"));
        Assert.assertEquals("e",sourceAsMap.get("letter"));
    }

    @Test
    public void minusCMinusDTwoFieldsAliasOnBothSecondTableFields() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        String query = String.format("SELECT pk , letter  FROM %s/systems WHERE system_name = 'C' " +
                "minus " +
                "SELECT myId as pk , myLetter as letter FROM %s/systems WHERE system_name = 'E' ",TEST_INDEX_SYSTEM,TEST_INDEX_SYSTEM);
        SearchHit[] searchHits = executeAndGetHits(query);
        Assert.assertEquals("not exactly one hit returned",1,searchHits.length);
        Map<String, Object> sourceAsMap = searchHits[0].getSourceAsMap();
        Assert.assertEquals("source map not contained exactly two fields",2,sourceAsMap.size());
        Assert.assertTrue("source map should contain pk",sourceAsMap.containsKey("pk"));
        Assert.assertTrue("source map should contain letter",sourceAsMap.containsKey("letter"));
        Assert.assertEquals(1,sourceAsMap.get("pk"));
        Assert.assertEquals("e",sourceAsMap.get("letter"));
    }


    @Test
    public void minusCMinusDTwoFieldsAliasOnBothTables() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        innerMinus_CMinusDTwoFieldsAliasOnBothTables("");
    }

    @Test
    public void minusCMinusDTwoFieldsAliasOnBothTablesWithScrolling() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        innerMinus_CMinusDTwoFieldsAliasOnBothTables(MINUS_SCROLL_DEFAULT_HINT);
    }

    private void innerMinus_CMinusDTwoFieldsAliasOnBothTables(String hint) throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = String.format("SELECT "+hint+" pk as myId , letter  FROM %s/systems WHERE system_name = 'C' " +
                "minus " +
                "SELECT myId , myLetter as letter FROM %s/systems WHERE system_name = 'E' ",TEST_INDEX_SYSTEM,TEST_INDEX_SYSTEM);
        SearchHit[] searchHits = executeAndGetHits(query);
        Assert.assertEquals("not exactly one hit returned", 1, searchHits.length);
        Map<String, Object> sourceAsMap = searchHits[0].getSourceAsMap();
        Assert.assertEquals("source map not contained exactly two fields",2,sourceAsMap.size());
        Assert.assertTrue("source map should contain pk",sourceAsMap.containsKey("myId"));
        Assert.assertTrue("source map should contain letter",sourceAsMap.containsKey("letter"));
        Assert.assertEquals(1,sourceAsMap.get("myId"));
        Assert.assertEquals("e",sourceAsMap.get("letter"));
    }


    @Test
    public void minusCMinusCTwoFields_OneAlias() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        String query = String.format("SELECT pk as myId , letter  FROM %s/systems WHERE system_name = 'C' " +
                "minus " +
                "SELECT pk as myId , letter FROM %s/systems WHERE system_name = 'C' ",TEST_INDEX_SYSTEM,TEST_INDEX_SYSTEM);
        SearchHit[] searchHits = executeAndGetHits(query);
        Assert.assertEquals("no hits should be returned", 0, searchHits.length);
    }

    @Test
    public void minusCMinusTNoExistsTwoFields() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        String query = String.format("SELECT pk , letter  FROM %s/systems WHERE system_name = 'C' " +
                "minus " +
                "SELECT pk  , letter FROM %s/systems WHERE system_name = 'T' ",TEST_INDEX_SYSTEM,TEST_INDEX_SYSTEM);
        SearchHit[] searchHits = executeAndGetHits(query);
        Assert.assertEquals("all hits should be returned",3,searchHits.length);
    }

    @Test
    public void minusCMinusTNoExistsOneField() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        innerMinus_CMinusTNoExistsOneField("");
    }

    @Test
    public void minusCMinusTNoExistsOneFieldWithScrolling() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        innerMinus_CMinusTNoExistsOneField(MINUS_SCROLL_DEFAULT_HINT);
    }

    @Test
    public void minusCMinusTNoExistsOneFieldWithScrollingAndOptimization() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        innerMinus_CMinusTNoExistsOneField(MINUS_SCROLL_DEFAULT_HINT + MINUS_TERMS_OPTIMIZATION_HINT);
    }


    private void innerMinus_CMinusTNoExistsOneField(String hint) throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = String.format("SELECT "+hint+" letter FROM %s/systems WHERE system_name = 'C' " +
                "minus " +
                "SELECT letter  FROM %s/systems WHERE system_name = 'T' ",TEST_INDEX_SYSTEM,TEST_INDEX_SYSTEM);
        SearchHit[] searchHits = executeAndGetHits(query);
        Assert.assertEquals("all hits should be returned", 3, searchHits.length);
    }

    @Test
    public void minusTMinusCNoExistsFirstQuery() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        innerMinus_TMinusCNoExistsFirstQuery("");
    }

    @Test
    public void minusTMinusCNoExistsFirstQueryWithScrolling() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        innerMinus_TMinusCNoExistsFirstQuery(MINUS_SCROLL_DEFAULT_HINT);
    }

    @Test
    public void minusTMinusCNoExistsFirstQueryWithScrollingAndOptimization() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        innerMinus_TMinusCNoExistsFirstQuery(MINUS_SCROLL_DEFAULT_HINT + MINUS_TERMS_OPTIMIZATION_HINT);
    }


    private void innerMinus_TMinusCNoExistsFirstQuery(String hint) throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = String.format("SELECT "+hint+" letter FROM %s/systems WHERE system_name = 'T' " +
                "minus " +
                "SELECT letter  FROM %s/systems WHERE system_name = 'C' ",TEST_INDEX_SYSTEM,TEST_INDEX_SYSTEM);
        SearchHit[] searchHits = executeAndGetHits(query);
        Assert.assertEquals("not hits should be returned", 0, searchHits.length);
    }

    @Test
    public void intersectWithComplexAlias() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        String query = String.format("SELECT firstname FROM %s/account " +
                "intersect " +
                "SELECT name.firstname as firstname FROM %s/gotCharacters", TEST_INDEX_ACCOUNT, TEST_INDEX_GAME_OF_THRONES);
        SearchHit[] searchHits = executeAndGetHits(query);
        Assert.assertEquals(1, searchHits.length);
        Assert.assertTrue(searchHits[0].getSourceAsMap().get("firstname").toString().contains("Jaime"));
    }

    private SearchHit[] executeAndGetHits(String query) throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        SearchDao searchDao = MainTestSuite.getSearchDao();
        SqlElasticRequestBuilder explain = searchDao.explain(query).explain();
        ElasticHitsExecutor executor  = MultiRequestExecutorFactory.createExecutor(searchDao.getClient(),(org.nlpcn.es4sql.query.multi.MultiQueryRequestBuilder) explain);
        executor.run();
        return executor.getHits().getHits();
    }

}
