package org.nlpcn.es4sql;


import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.plugin.nlpcn.HashJoinElasticExecutor;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.Test;
import org.junit.Assert;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.HashJoinElasticRequestBuilder;
import org.nlpcn.es4sql.query.SqlElasticRequestBuilder;
import org.nlpcn.es4sql.query.SqlElasticSearchRequestBuilder;

import java.io.IOException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;
import java.util.Map;

import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX;

/**
 * Created by Eliran on 22/8/2015.
 */
public class JoinTests {

    @Test
    public void joinParseCheckSelectedFieldsSplit() throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = "SELECT a.firstname ,a.lastname , a.gender ,d.name  FROM elasticsearch-sql_test_index/people a " +
                " JOIN elasticsearch-sql_test_index/dog d on d.holdersName = a.firstname " +
                " WHERE " +
                " (a.age > 10 OR a.balance > 2000)" +
                " AND d.age > 1";
        SearchHit[] hits = hashJoinGetHits(query);
        Assert.assertEquals(2,hits.length);

        Map<String,Object> oneMatch = ImmutableMap.of("a.firstname", (Object)"Daenerys", "a.lastname","Targaryen",
                                                        "a.gender","M","d.name", "rex");
        Map<String,Object> secondMatch = ImmutableMap.of("a.firstname", (Object) "Hattie", "a.lastname", "Bond",
                "a.gender", "M", "d.name", "snoopy");

        Assert.assertTrue(hitsContains(hits, oneMatch));
        Assert.assertTrue(hitsContains(hits,secondMatch));

    }

    @Test
    public void joinParseWithHintsCheckSelectedFieldsSplit() throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = "SELECT /*! HASH_WITH_TERMS_FILTER*/ a.firstname ,a.lastname , a.gender ,d.name  FROM elasticsearch-sql_test_index/people a " +
                " JOIN elasticsearch-sql_test_index/dog d on d.holdersName = a.firstname " +
                " WHERE " +
                " (a.age > 10 OR a.balance > 2000)" +
                " AND d.age > 1";
        String explainedQuery = hashJoinRunAndExplain(query);
        boolean containTerms = explainedQuery.replaceAll("\\s+","").contains("\"terms\":{\"holdersName\":[\"daenerys\",\"hattie\",\"nanette\",\"dale\",\"elinor\",\"virginia\",\"dillard\",\"mcgee\",\"aurelia\",\"fulton\",\"burton\"]}");
        Assert.assertTrue(containTerms);
    }

    private String hashJoinRunAndExplain(String query) throws IOException, SqlParseException, SQLFeatureNotSupportedException {
        SearchDao searchDao = MainTestSuite.getSearchDao();
        HashJoinElasticRequestBuilder explain = (HashJoinElasticRequestBuilder) searchDao.explain(query);
        HashJoinElasticExecutor executor = new HashJoinElasticExecutor(searchDao.getClient(),  explain);
        executor.run();
        return explain.explain();
    }
    private SearchHit[] hashJoinGetHits(String query) throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        SearchDao searchDao = MainTestSuite.getSearchDao();
        SqlElasticRequestBuilder explain = searchDao.explain(query);
        HashJoinElasticExecutor executor = new HashJoinElasticExecutor(searchDao.getClient(), (HashJoinElasticRequestBuilder) explain);
        executor.run();
        return executor.getHits().getHits();
    }

    private boolean hitsContains(SearchHit[] hits, Map<String, Object> matchMap) {
        for(SearchHit hit : hits){
            Map<String, Object> hitMap = hit.sourceAsMap();
            boolean matchedHit = true;
            for(Map.Entry<String,Object> entry: hitMap.entrySet()){
                if(!matchMap.containsKey(entry.getKey())) {
                    matchedHit = false;
                    break;
                }
                if(!equalsWithNullCheck(matchMap.get(entry.getKey()), entry.getValue())){
                    matchedHit = false;
                    break;
                }
            }
            if(matchedHit) return true;
        }
        return false;
    }

    private boolean equalsWithNullCheck(Object one, Object other) {
        if(one == null)   return other == null;
        return one.equals(other);
    }


    @Test
    public void joinWithNoWhereButWithCondition() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        String query = String.format("select c.gender , h.name,h.words from %s/gotCharacters c " +
                "JOIN %s/gotHouses h " +
                "on c.house = h.name",TEST_INDEX,TEST_INDEX);
        SearchHit[] hits = hashJoinGetHits(query);
        Assert.assertEquals(4,hits.length);
        Map<String,Object> someMatch =  ImmutableMap.of("c.gender", (Object)"F", "h.name","Targaryen",
                "h.words","fireAndBlood");
        Assert.assertTrue(hitsContains(hits, someMatch));
    }

    @Test
    public void joinNoConditionButWithWhere() throws SQLFeatureNotSupportedException, IOException, SqlParseException {

        String query = String.format("select c.gender , h.name,h.words from %s/gotCharacters c " +
                "JOIN %s/gotHouses h " +
                "where c.name.firstname='Daenerys'",TEST_INDEX,TEST_INDEX);
        SearchHit[] hits = hashJoinGetHits(query);
        Assert.assertEquals(3,hits.length);

    }

    @Test
    public void joinNoConditionAndNoWhere() throws SQLFeatureNotSupportedException, IOException, SqlParseException {

        String query = String.format("select c.name.firstname,c.parents.father , h.name,h.words from %s/gotCharacters c " +
                "JOIN %s/gotHouses h ",TEST_INDEX,TEST_INDEX);
        SearchHit[] hits = hashJoinGetHits(query);
        Assert.assertEquals(12,hits.length);

    }


    @Test
    public void joinNoConditionAndNoWhereWithTotalLimit() throws SQLFeatureNotSupportedException, IOException, SqlParseException {

        String query = String.format("select c.name.firstname,c.parents.father , h.name,h.words from %s/gotCharacters c " +
                "JOIN %s/gotHouses h LIMIT 10",TEST_INDEX,TEST_INDEX);
        SearchHit[] hits = hashJoinGetHits(query);
        Assert.assertEquals(10,hits.length);

    }

    @Test
    public void joinWithNestedFieldsOnReturn() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        String query = String.format("select c.name.firstname,c.parents.father , h.name,h.words from %s/gotCharacters c " +
                "JOIN %s/gotHouses h " +
                "on c.house = h.name " +
                "where c.name.firstname='Daenerys'", TEST_INDEX,TEST_INDEX);
        SearchHit[] hits = hashJoinGetHits(query);
        Assert.assertEquals(1,hits.length);
        //use flatten?
        Map<String,Object> someMatch =  ImmutableMap.of("c.name.firstname", (Object)"Daenerys","c.parents.father","Aerys", "h.name","Targaryen",
                "h.words","fireAndBlood");
        Assert.assertTrue(hitsContains(hits, someMatch));
    }

    @Test
    public void joinWithNestedFieldsOnComparisonAndOnReturn() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        String query = String.format("select c.name.firstname,c.parents.father , h.name,h.words from %s/gotCharacters c " +
                "JOIN %s/gotHouses h " +
                "on c.name.lastname = h.name " +
                "where c.name.firstname='Daenerys'", TEST_INDEX,TEST_INDEX);
        SearchHit[] hits = hashJoinGetHits(query);
        Assert.assertEquals(1,hits.length);
        Map<String,Object> someMatch =  ImmutableMap.of("c.name.firstname", (Object)"Daenerys","c.parents.father","Aerys", "h.name","Targaryen",
                "h.words","fireAndBlood");
        Assert.assertTrue(hitsContains(hits, someMatch));
    }


    @Test
    public void testLeftJoin() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        String query = String.format("select c.name.firstname, f.name.firstname,f.name.lastname from %s/gotCharacters c " +
                "LEFT JOIN %s/gotCharacters f " +
                "on c.parents.father = f.name.firstname "
                , TEST_INDEX,TEST_INDEX);
        SearchHit[] hits = hashJoinGetHits(query);
        Assert.assertEquals(4,hits.length);

        Map<String,Object> oneMatch = new HashMap<>();
        oneMatch.put("c.name.firstname", "Daenerys");
        oneMatch.put("f.name.firstname",null);
        oneMatch.put("f.name.lastname",null);

        Assert.assertTrue(hitsContains(hits, oneMatch));
        Map<String,Object> secondMatch =  ImmutableMap.of("c.name.firstname", (Object)"Brandon",
                "f.name.firstname","Eddard", "f.name.lastname","Stark");
        Assert.assertTrue(hitsContains(hits, secondMatch));
    }

    @Test
    public void hintLimits_firstLimitSecondNull() throws SQLFeatureNotSupportedException, IOException, SqlParseException {

        String query = String.format("select /*! JOIN_TABLES_LIMIT(2,null) */ c.name.firstname,c.parents.father , h.name,h.words from %s/gotCharacters c " +
                "JOIN %s/gotHouses h ",TEST_INDEX,TEST_INDEX);
        SearchHit[] hits = hashJoinGetHits(query);
        Assert.assertEquals(6,hits.length);
    }

    @Test
    public void hintLimits_firstLimitSecondLimit() throws SQLFeatureNotSupportedException, IOException, SqlParseException {

        String query = String.format("select /*! JOIN_TABLES_LIMIT(2,2) */ c.name.firstname,c.parents.father , h.name,h.words from %s/gotCharacters c " +
                "JOIN %s/gotHouses h ",TEST_INDEX,TEST_INDEX);
        SearchHit[] hits = hashJoinGetHits(query);
        Assert.assertEquals(4,hits.length);
    }

    @Test
    public void hintLimits_firstNullSecondLimit() throws SQLFeatureNotSupportedException, IOException, SqlParseException {

        String query = String.format("select /*! JOIN_TABLES_LIMIT(null,2) */ c.name.firstname,c.parents.father , h.name,h.words from %s/gotCharacters c " +
                "JOIN %s/gotHouses h ",TEST_INDEX,TEST_INDEX);
        SearchHit[] hits = hashJoinGetHits(query);
        Assert.assertEquals(8,hits.length);
    }

    @Test
    public void testLeftJoinWithLimit() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        String query = String.format("select /*! JOIN_TABLES_LIMIT(3,null) */ c.name.firstname, f.name.firstname,f.name.lastname from %s/gotCharacters c " +
                "LEFT JOIN %s/gotCharacters f " +
                "on c.parents.father = f.name.firstname "
                , TEST_INDEX,TEST_INDEX);
        SearchHit[] hits = hashJoinGetHits(query);
        Assert.assertEquals(3,hits.length);

    }


}
