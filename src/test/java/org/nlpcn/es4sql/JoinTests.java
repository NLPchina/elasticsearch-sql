package org.nlpcn.es4sql;


import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.plugin.nlpcn.ElasticJoinExecutor;
import org.elasticsearch.plugin.nlpcn.HashJoinElasticExecutor;
import org.elasticsearch.search.SearchHit;
import org.junit.Test;
import org.junit.Assert;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.join.HashJoinElasticRequestBuilder;
import org.nlpcn.es4sql.query.SqlElasticRequestBuilder;

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
    public void joinParseCheckSelectedFieldsSplitHASH() throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        joinParseCheckSelectedFieldsSplit(false);
    }

    @Test
    public void joinParseCheckSelectedFieldsSplitNL() throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        joinParseCheckSelectedFieldsSplit(true);
    }

    private void joinParseCheckSelectedFieldsSplit(boolean useNestedLoops) throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = "SELECT a.firstname ,a.lastname , a.gender ,d.name  FROM elasticsearch-sql_test_index/people a " +
                " JOIN elasticsearch-sql_test_index/dog d on d.holdersName = a.firstname " +
                " WHERE " +
                " (a.age > 10 OR a.balance > 2000)" +
                " AND d.age > 1";
        if(useNestedLoops) query = query.replace("SELECT","SELECT /*! USE_NL*/ ");
        SearchHit[] hits = joinAndGetHits(query);
        Assert.assertEquals(2, hits.length);

        Map<String,Object> oneMatch = ImmutableMap.of("a.firstname", (Object) "Daenerys", "a.lastname", "Targaryen",
                "a.gender", "M", "d.name", "rex");
        Map<String,Object> secondMatch = ImmutableMap.of("a.firstname", (Object) "Hattie", "a.lastname", "Bond",
                "a.gender", "M", "d.name", "snoopy");

        Assert.assertTrue(hitsContains(hits, oneMatch));
        Assert.assertTrue(hitsContains(hits,secondMatch));
    }

    @Test
    public void joinParseWithHintsCheckSelectedFieldsSplitHASH() throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = "SELECT /*! HASH_WITH_TERMS_FILTER*/ a.firstname ,a.lastname , a.gender ,d.name  FROM elasticsearch-sql_test_index/people a " +
                " JOIN elasticsearch-sql_test_index/dog d on d.holdersName = a.firstname " +
                " WHERE " +
                " (a.age > 10 OR a.balance > 2000)" +
                " AND d.age > 1";
        String explainedQuery = hashJoinRunAndExplain(query);
        boolean containTerms = explainedQuery.replaceAll("\\s+","").contains("\"terms\":{\"holdersName\":[\"daenerys\",\"hattie\",\"nanette\",\"dale\",\"elinor\",\"virginia\",\"dillard\",\"mcgee\",\"aurelia\",\"fulton\",\"burton\"]}");
        Assert.assertTrue(containTerms);
    }


    @Test
    public void joinWithNoWhereButWithConditionHash() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        joinWithNoWhereButWithCondition(false);
    }

    @Test
    public void joinWithNoWhereButWithConditionNL() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        joinWithNoWhereButWithCondition(true);
    }

    private void joinWithNoWhereButWithCondition(boolean useNestedLoops) throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = String.format("select c.gender , h.name,h.words from %s/gotCharacters c " +
                "JOIN %s/gotHouses h " +
                "on h.name = c.house ",TEST_INDEX,TEST_INDEX);
        if(useNestedLoops) query = query.replace("select","select /*! USE_NL*/ ");
        SearchHit[] hits = joinAndGetHits(query);
        Assert.assertEquals(4, hits.length);
        Map<String,Object> someMatch =  ImmutableMap.of("c.gender", (Object) "F", "h.name", "Targaryen",
                "h.words", "fireAndBlood");
        Assert.assertTrue(hitsContains(hits, someMatch));
    }

    @Test
    public void joinNoConditionButWithWhereHASH() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        joinNoConditionButWithWhere(false);
    }
    @Test
    public void joinNoConditionButWithWhereNL() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        joinNoConditionButWithWhere(true);
    }

    private void joinNoConditionButWithWhere(boolean useNestedLoops) throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = String.format("select c.gender , h.name,h.words from %s/gotCharacters c " +
                "JOIN %s/gotHouses h " +
                "where c.name.firstname='Daenerys'",TEST_INDEX,TEST_INDEX);
        if(useNestedLoops) query = query.replace("select","select /*! USE_NL*/ ");
        SearchHit[] hits = joinAndGetHits(query);
        Assert.assertEquals(3, hits.length);
    }

    @Test
    public void joinNoConditionAndNoWhereHASH() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        joinNoConditionAndNoWhere(false);
    }

    @Test
    public void joinNoConditionAndNoWhereNL() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        joinNoConditionAndNoWhere(true);
    }

    private void joinNoConditionAndNoWhere(boolean useNestedLoops) throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = String.format("select c.name.firstname,c.parents.father , h.name,h.words from %s/gotCharacters c " +
                "JOIN %s/gotHouses h ",TEST_INDEX,TEST_INDEX);
        if(useNestedLoops) query = query.replace("select","select /*! USE_NL*/ ");
        SearchHit[] hits = joinAndGetHits(query);
        Assert.assertEquals(12, hits.length);
    }

    @Test
    public void joinNoConditionAndNoWhereWithTotalLimitHASH() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        joinNoConditionAndNoWhereWithTotalLimit(false);
    }

    @Test
    public void joinNoConditionAndNoWhereWithTotalLimitNL() throws SQLFeatureNotSupportedException, IOException, SqlParseException {

        joinNoConditionAndNoWhereWithTotalLimit(true);

    }

    private void joinNoConditionAndNoWhereWithTotalLimit(boolean useNestedLoops) throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = String.format("select c.name.firstname,c.parents.father , h.name,h.words from %s/gotCharacters c " +
                "JOIN %s/gotHouses h LIMIT 10",TEST_INDEX,TEST_INDEX);
        if(useNestedLoops) query = query.replace("select","select /*! USE_NL*/ ");
        SearchHit[] hits = joinAndGetHits(query);
        Assert.assertEquals(10, hits.length);
    }

    @Test
    public void joinWithNestedFieldsOnReturnHASH() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        joinWithNestedFieldsOnReturn(false);
    }

    @Test
    public void joinWithNestedFieldsOnReturnNL() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        joinWithNestedFieldsOnReturn(true);
    }

    private void joinWithNestedFieldsOnReturn(boolean useNestedLoops) throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = String.format("select c.name.firstname,c.parents.father , h.name,h.words from %s/gotCharacters c " +
                "JOIN %s/gotHouses h " +
                "on h.name = c.house " +
                "where c.name.firstname='Daenerys'", TEST_INDEX,TEST_INDEX);
        if(useNestedLoops) query = query.replace("select","select /*! USE_NL*/ ");
        SearchHit[] hits = joinAndGetHits(query);
        Assert.assertEquals(1, hits.length);
        //use flatten?
        Map<String,Object> someMatch =  ImmutableMap.of("c.name.firstname", (Object) "Daenerys", "c.parents.father", "Aerys", "h.name", "Targaryen",
                "h.words", "fireAndBlood");
        Assert.assertTrue(hitsContains(hits, someMatch));
    }

    @Test
    public void joinWithNestedFieldsOnComparisonAndOnReturnHASH() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        joinWithNestedFieldsOnComparisonAndOnReturn(false);
    }

    @Test
    public void joinWithNestedFieldsOnComparisonAndOnReturnNL() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        joinWithNestedFieldsOnComparisonAndOnReturn(true);
    }

    private void joinWithNestedFieldsOnComparisonAndOnReturn(boolean useNestedLoops) throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = String.format("select c.name.firstname,c.parents.father , h.name,h.words from %s/gotCharacters c " +
                "JOIN %s/gotHouses h " +
                "on h.name = c.name.lastname " +
                "where c.name.firstname='Daenerys'", TEST_INDEX,TEST_INDEX);
        if(useNestedLoops) query = query.replace("select","select /*! USE_NL*/ ");
        SearchHit[] hits = joinAndGetHits(query);
        Assert.assertEquals(1, hits.length);
        Map<String,Object> someMatch =  ImmutableMap.of("c.name.firstname", (Object) "Daenerys", "c.parents.father", "Aerys", "h.name", "Targaryen",
                "h.words", "fireAndBlood");
        Assert.assertTrue(hitsContains(hits, someMatch));
    }


    @Test
    public void testLeftJoinHASH() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        testLeftJoin(false);
    }

    @Test
    public void testLeftJoinNL() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        testLeftJoin(true);
    }

    private void testLeftJoin(boolean useNestedLoops) throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = String.format("select c.name.firstname, f.name.firstname,f.name.lastname from %s/gotCharacters c " +
                "LEFT JOIN %s/gotCharacters f " +
                "on f.name.firstname = c.parents.father "
                , TEST_INDEX,TEST_INDEX);
        if(useNestedLoops) query = query.replace("select","select /*! USE_NL*/ ");
        SearchHit[] hits = joinAndGetHits(query);
        Assert.assertEquals(4, hits.length);

        Map<String,Object> oneMatch = new HashMap<>();
        oneMatch.put("c.name.firstname", "Daenerys");
        oneMatch.put("f.name.firstname",null);
        oneMatch.put("f.name.lastname",null);

        Assert.assertTrue(hitsContains(hits, oneMatch));
        Map<String,Object> secondMatch =  ImmutableMap.of("c.name.firstname", (Object) "Brandon",
                "f.name.firstname", "Eddard", "f.name.lastname", "Stark");
        Assert.assertTrue(hitsContains(hits, secondMatch));
    }

    @Test
    public void hintLimits_firstLimitSecondNullHASH() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        hintLimits_firstLimitSecondNull(false);
    }

    @Test
    public void hintLimits_firstLimitSecondNullNL() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        hintLimits_firstLimitSecondNull(true);
    }

    private void hintLimits_firstLimitSecondNull(boolean useNestedLoops) throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = String.format("select /*! JOIN_TABLES_LIMIT(2,null) */ c.name.firstname,c.parents.father , h.name,h.words from %s/gotCharacters c " +
                "JOIN %s/gotHouses h ",TEST_INDEX,TEST_INDEX);
        if(useNestedLoops) query = query.replace("select","select /*! USE_NL*/ ");
        SearchHit[] hits = joinAndGetHits(query);
        Assert.assertEquals(6, hits.length);
    }

    @Test
    public void hintLimits_firstLimitSecondLimitHASH() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        hintLimits_firstLimitSecondLimit(false);
    }

    @Test
    public void hintLimits_firstLimitSecondLimitNL() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        hintLimits_firstLimitSecondLimit(true);
    }

    private void hintLimits_firstLimitSecondLimit(boolean useNestedLoops) throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = String.format("select /*! JOIN_TABLES_LIMIT(2,2) */ c.name.firstname,c.parents.father , h.name,h.words from %s/gotCharacters c " +
                "JOIN %s/gotHouses h ",TEST_INDEX,TEST_INDEX);
        if(useNestedLoops) query = query.replace("select","select /*! USE_NL*/ ");
        SearchHit[] hits = joinAndGetHits(query);
        Assert.assertEquals(4, hits.length);
    }

    @Test
    public void hintLimits_firstLimitSecondLimitOnlyOneNL() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        hintLimits_firstLimitSecondLimitOnlyOne(true);
    }

    @Test
    public void hintLimits_firstLimitSecondLimitOnlyOneHASH() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        hintLimits_firstLimitSecondLimitOnlyOne(false);
    }

    private void hintLimits_firstLimitSecondLimitOnlyOne(boolean useNestedLoops) throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = String.format("select /*! JOIN_TABLES_LIMIT(3,1) */ c.name.firstname,c.parents.father , h.name,h.words from %s/gotHouses h " +
                "JOIN  %s/gotCharacters c  ON c.name.lastname = h.name ",TEST_INDEX,TEST_INDEX);
        if(useNestedLoops) query = query.replace("select","select /*! USE_NL*/ ");
        SearchHit[] hits = joinAndGetHits(query);
        if(useNestedLoops) Assert.assertEquals(3, hits.length);
        else Assert.assertEquals(1, hits.length);
    }

    @Test
    public void hintLimits_firstNullSecondLimitHASH() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        hintLimits_firstNullSecondLimit(false);
    }

    @Test
    public void hintLimits_firstNullSecondLimitNL() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        hintLimits_firstNullSecondLimit(true);
    }

    private void hintLimits_firstNullSecondLimit(boolean useNestedLoops) throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = String.format("select /*! JOIN_TABLES_LIMIT(null,2) */ c.name.firstname,c.parents.father , h.name,h.words from %s/gotCharacters c " +
                "JOIN %s/gotHouses h ",TEST_INDEX,TEST_INDEX);
        if(useNestedLoops) query = query.replace("select","select /*! USE_NL*/ ");
        SearchHit[] hits = joinAndGetHits(query);
        Assert.assertEquals(8, hits.length);
    }

    @Test
    public void testLeftJoinWithLimitHASH() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        testLeftJoinWithLimit(false);
    }

    @Test
    public void testLeftJoinWithLimitNL() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        testLeftJoinWithLimit(true);
    }

    private void testLeftJoinWithLimit(boolean useNestedLoops) throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = String.format("select /*! JOIN_TABLES_LIMIT(3,null) */ c.name.firstname, f.name.firstname,f.name.lastname from %s/gotCharacters c " +
                "LEFT JOIN %s/gotCharacters f " +
                "on f.name.firstname = c.parents.father"
                , TEST_INDEX,TEST_INDEX);
        if(useNestedLoops) query = query.replace("select","select /*! USE_NL*/ ");
        SearchHit[] hits = joinAndGetHits(query);
        Assert.assertEquals(3, hits.length);
    }

    @Test
    public void hintMultiSearchCanRunFewTimesNL() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        String query = String.format("select /*! USE_NL*/ /*! NL_MULTISEARCH_SIZE(2)*/ c.name.firstname,c.parents.father , h.name,h.words from %s/gotCharacters c " +
                "JOIN %s/gotHouses h ",TEST_INDEX,TEST_INDEX);
        SearchHit[] hits = joinAndGetHits(query);
        Assert.assertEquals(12, hits.length);
    }

    @Test
    public void joinWithGeoIntersectNL() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        String query = String.format("select p1.description,p2.description from %s/location p1 " +
                "JOIN %s/location2 p2 " +
                "ON GEO_INTERSECTS(p2.place,p1.place)",TEST_INDEX,TEST_INDEX);
        SearchHit[] hits = joinAndGetHits(query);
        Assert.assertEquals(2, hits.length);
        Assert.assertEquals("squareRelated",hits[0].getSource().get("p2.description"));
        Assert.assertEquals("squareRelated",hits[1].getSource().get("p2.description"));
    }


    private String hashJoinRunAndExplain(String query) throws IOException, SqlParseException, SQLFeatureNotSupportedException {
        SearchDao searchDao = MainTestSuite.getSearchDao();
        HashJoinElasticRequestBuilder explain = (HashJoinElasticRequestBuilder) searchDao.explain(query);
        HashJoinElasticExecutor executor = new HashJoinElasticExecutor(searchDao.getClient(),  explain);
        executor.run();
        return explain.explain();
    }

    private SearchHit[] joinAndGetHits(String query) throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        SearchDao searchDao = MainTestSuite.getSearchDao();
        SqlElasticRequestBuilder explain = searchDao.explain(query);
        ElasticJoinExecutor executor  = ElasticJoinExecutor.createJoinExecutor(searchDao.getClient(),explain);
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

}
