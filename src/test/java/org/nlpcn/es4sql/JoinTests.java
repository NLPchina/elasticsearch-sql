package org.nlpcn.es4sql;


import com.google.common.collect.ImmutableMap;
import org.elasticsearch.plugin.nlpcn.ElasticJoinExecutor;
import org.elasticsearch.plugin.nlpcn.HashJoinElasticExecutor;
import org.elasticsearch.search.SearchHit;
import org.junit.Assert;
import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.SqlElasticRequestBuilder;
import org.nlpcn.es4sql.query.join.HashJoinElasticRequestBuilder;

import java.io.IOException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_DOG;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_GAME_OF_THRONES;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_LOCATION;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_LOCATION2;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_PEOPLE;

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
        String query = "SELECT a.firstname ,a.lastname , a.gender ,d.dog_name  FROM " +
                TEST_INDEX_PEOPLE +
                "/people a " +
                " JOIN " +
                TEST_INDEX_DOG +
                "/dog d on d.holdersName = a.firstname " +
                " WHERE " +
                " (a.age > 10 OR a.balance > 2000)" +
                " AND d.age > 1";
        if(useNestedLoops) query = query.replace("SELECT","SELECT /*! USE_NL*/ ");
        SearchHit[] hits = joinAndGetHits(query);
        Assert.assertEquals(2, hits.length);

        Map<String,Object> oneMatch = ImmutableMap.of("a.firstname", (Object) "Daenerys", "a.lastname", "Targaryen",
                "a.gender", "M", "d.dog_name", "rex");
        Map<String,Object> secondMatch = ImmutableMap.of("a.firstname", (Object) "Hattie", "a.lastname", "Bond",
                "a.gender", "M", "d.dog_name", "snoopy");

        Assert.assertTrue(hitsContains(hits, oneMatch));
        Assert.assertTrue(hitsContains(hits,secondMatch));
    }

    @Test
    public void joinParseWithHintsCheckSelectedFieldsSplitHASH() throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = "SELECT /*! HASH_WITH_TERMS_FILTER*/ a.firstname ,a.lastname , a.gender ,d.dog_name  FROM " +
                TEST_INDEX_PEOPLE +
                "/people a " +
                " JOIN " +
                TEST_INDEX_DOG +
                "/dog d on d.holdersName = a.firstname " +
                " WHERE " +
                " (a.age > 10 OR a.balance > 2000)" +
                " AND d.age > 1";
        String explainedQuery = hashJoinRunAndExplain(query);
        boolean containTerms = explainedQuery.replaceAll("\\s+","").contains("\"terms\":{\"holdersName\":[");
        List<String> holdersName = Arrays.asList("daenerys","nanette","virginia","aurelia","mcgee","hattie","elinor","burton");
        for(String holderName : holdersName){
            Assert.assertTrue("should contain:" + holderName , explainedQuery.contains(holderName));
        }
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
        String query = String.format("select c.gender , h.hname,h.words from %s/gotCharacters c " +
                "JOIN %s/gotCharacters h " +
                "on h.hname = c.house ",TEST_INDEX_GAME_OF_THRONES,TEST_INDEX_GAME_OF_THRONES);
        if(useNestedLoops) query = query.replace("select","select /*! USE_NL*/ ");
        SearchHit[] hits = joinAndGetHits(query);
        Assert.assertEquals(16, hits.length);
        Map<String,Object> someMatch =  ImmutableMap.of("c.gender", (Object) "F", "h.hname", "Targaryen",
                "h.words", "fireAndBlood");
        Assert.assertTrue(hitsContains(hits, someMatch));
    }

    @Test
    public void joinWithStarASH() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        joinWithStar(false);
    }

    private void joinWithStar(boolean useNestedLoops) throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = String.format("select * from %s/gotCharacters c " +
                "JOIN %s/gotCharacters h " +
                "on h.hname = c.house ",TEST_INDEX_GAME_OF_THRONES,TEST_INDEX_GAME_OF_THRONES);
        if(useNestedLoops) query = query.replace("select","select /*! USE_NL*/ ");
        SearchHit[] hits = joinAndGetHits(query);
        Assert.assertEquals(16, hits.length);
        SearchHit hit = Stream.of(hits).filter(h -> "1|5".equals(h.getId())).findAny().get();
        String house = hit.getSourceAsMap().get("c.house").toString();
        boolean someHouse = house.equals("Targaryen") || house.equals("Stark") || house.equals("Lannister");
        Assert.assertTrue(someHouse);
        String houseName = hit.getSourceAsMap().get("h.hname").toString();
        Assert.assertEquals(house, houseName);
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
        String query = String.format("select c.gender , h.hname,h.words from %s/gotCharacters c " +
                "JOIN %s/gotCharacters h " +
                "where c.name.firstname='Daenerys'",TEST_INDEX_GAME_OF_THRONES,TEST_INDEX_GAME_OF_THRONES);
        if(useNestedLoops) query = query.replace("select","select /*! USE_NL*/ ");
        SearchHit[] hits = joinAndGetHits(query);
        Assert.assertEquals(7, hits.length);
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
        String query = String.format("select c.name.firstname,c.parents.father , h.hname,h.words from %s/gotCharacters c " +
                "JOIN %s/gotCharacters h ",TEST_INDEX_GAME_OF_THRONES,TEST_INDEX_GAME_OF_THRONES);
        if(useNestedLoops) query = query.replace("select","select /*! USE_NL*/ ");
        SearchHit[] hits = joinAndGetHits(query);
        Assert.assertEquals(49, hits.length);
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
        String query = String.format("select c.name.firstname,c.parents.father , h.hname,h.words from %s/gotCharacters c " +
                "JOIN %s/gotCharacters h LIMIT 10",TEST_INDEX_GAME_OF_THRONES,TEST_INDEX_GAME_OF_THRONES);
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
        String query = String.format("select c.name.firstname,c.parents.father , h.hname,h.words from %s/gotCharacters c " +
                "JOIN %s/gotCharacters h " +
                "on h.hname = c.house " +
                "where c.name.firstname='Daenerys'", TEST_INDEX_GAME_OF_THRONES,TEST_INDEX_GAME_OF_THRONES);
        if(useNestedLoops) query = query.replace("select","select /*! USE_NL*/ ");
        SearchHit[] hits = joinAndGetHits(query);
        Assert.assertEquals(1, hits.length);
        //use flatten?
        Map<String,Object> someMatch =  ImmutableMap.of("c.name.firstname", (Object) "Daenerys", "c.parents.father", "Aerys", "h.hname", "Targaryen",
                "h.words", "fireAndBlood");
        Assert.assertTrue(hitsContains(hits, someMatch));
    }

    @Test
    public void joinWithAllAliasOnReturnHASH() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        joinWithAllAliasOnReturn(false);
    }
    @Test
    public void joinWithAllAliasOnReturnNL() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        joinWithAllAliasOnReturn(true);
    }

    private void joinWithAllAliasOnReturn(boolean useNestedLoops)  throws SqlParseException, SQLFeatureNotSupportedException, IOException {
            String query = String.format("select c.name.firstname name,c.parents.father father, h.hname house from %s/gotCharacters c " +
                    "JOIN %s/gotCharacters h " +
                    "on h.hname = c.house " +
                    "where c.name.firstname='Daenerys'", TEST_INDEX_GAME_OF_THRONES,TEST_INDEX_GAME_OF_THRONES);
            if(useNestedLoops) query = query.replace("select","select /*! USE_NL*/ ");
            SearchHit[] hits = joinAndGetHits(query);
            Assert.assertEquals(1, hits.length);

            Map<String,Object> someMatch =  ImmutableMap.of("name", (Object) "Daenerys", "father", "Aerys", "house", "Targaryen");
            Assert.assertTrue(hitsContains(hits, someMatch));
    }

    @Test
    public void joinWithSomeAliasOnReturnHASH() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        joinWithSomeAliasOnReturn(false);
    }
    @Test
    public void joinWithSomeAliasOnReturnNL() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        joinWithSomeAliasOnReturn(true);
    }

    private void joinWithSomeAliasOnReturn(boolean useNestedLoops)  throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = String.format("select c.name.firstname ,c.parents.father father, h.hname house from %s/gotCharacters c " +
                "JOIN %s/gotCharacters h " +
                "on h.hname = c.house " +
                "where c.name.firstname='Daenerys'", TEST_INDEX_GAME_OF_THRONES,TEST_INDEX_GAME_OF_THRONES);
        if(useNestedLoops) query = query.replace("select","select /*! USE_NL*/ ");
        SearchHit[] hits = joinAndGetHits(query);
        Assert.assertEquals(1, hits.length);

        Map<String,Object> someMatch =  ImmutableMap.of("c.name.firstname", (Object) "Daenerys", "father", "Aerys", "house", "Targaryen");
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
        String query = String.format("select c.name.firstname,c.parents.father , h.hname,h.words from %s/gotCharacters c " +
                "JOIN %s/gotCharacters h " +
                "on h.hname = c.name.lastname " +
                "where c.name.firstname='Daenerys'", TEST_INDEX_GAME_OF_THRONES,TEST_INDEX_GAME_OF_THRONES);
        if(useNestedLoops) query = query.replace("select","select /*! USE_NL*/ ");
        SearchHit[] hits = joinAndGetHits(query);
        Assert.assertEquals(1, hits.length);
        Map<String,Object> someMatch =  ImmutableMap.of("c.name.firstname", (Object) "Daenerys", "c.parents.father", "Aerys", "h.hname", "Targaryen",
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
                , TEST_INDEX_GAME_OF_THRONES,TEST_INDEX_GAME_OF_THRONES);
        if(useNestedLoops) query = query.replace("select","select /*! USE_NL*/ ");
        SearchHit[] hits = joinAndGetHits(query);
        Assert.assertEquals(13, hits.length);

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
        String query = String.format("select /*! JOIN_TABLES_LIMIT(2,null) */ c.name.firstname,c.parents.father , h.hname,h.words from %s/gotCharacters c " +
                "JOIN %s/gotCharacters h ",TEST_INDEX_GAME_OF_THRONES,TEST_INDEX_GAME_OF_THRONES);
        if(useNestedLoops) query = query.replace("select","select /*! USE_NL*/ ");
        SearchHit[] hits = joinAndGetHits(query);
        Assert.assertEquals(14, hits.length);
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
        String query = String.format("select /*! JOIN_TABLES_LIMIT(2,2) */ c.name.firstname,c.parents.father , h.hname,h.words from %s/gotCharacters c " +
                "JOIN %s/gotCharacters h ",TEST_INDEX_GAME_OF_THRONES,TEST_INDEX_GAME_OF_THRONES);
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
        String query = String.format("select /*! JOIN_TABLES_LIMIT(100,100) */ c.name.firstname,c.parents.father , h.hname,h.words from %s/gotCharacters h " +
                "JOIN  %s/gotCharacters c  ON c.name.lastname = h.hname ",TEST_INDEX_GAME_OF_THRONES,TEST_INDEX_GAME_OF_THRONES);
        if(useNestedLoops) query = query.replace("select","select /*! USE_NL*/ ");
        SearchHit[] hits = joinAndGetHits(query);
        if(useNestedLoops) Assert.assertEquals(16, hits.length);
        else Assert.assertEquals(16, hits.length);
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
        String query = String.format("select /*! JOIN_TABLES_LIMIT(null,2) */ c.name.firstname,c.parents.father , h.hname,h.words from %s/gotCharacters c " +
                "JOIN %s/gotCharacters h ",TEST_INDEX_GAME_OF_THRONES,TEST_INDEX_GAME_OF_THRONES);
        if(useNestedLoops) query = query.replace("select","select /*! USE_NL*/ ");
        SearchHit[] hits = joinAndGetHits(query);
        Assert.assertEquals(14, hits.length);
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
        String query = String.format("select /*! JOIN_TABLES_LIMIT(100,null) */ c.name.firstname, f.name.firstname,f.name.lastname from %s/gotCharacters c " +
                "LEFT JOIN %s/gotCharacters f " +
                "on f.name.firstname = c.parents.father"
                , TEST_INDEX_GAME_OF_THRONES,TEST_INDEX_GAME_OF_THRONES);
        if(useNestedLoops) query = query.replace("select","select /*! USE_NL*/ ");
        SearchHit[] hits = joinAndGetHits(query);
        Assert.assertEquals(13, hits.length);
    }

    @Test
    public void hintMultiSearchCanRunFewTimesNL() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        String query = String.format("select /*! USE_NL*/ /*! NL_MULTISEARCH_SIZE(2)*/ c.name.firstname,c.parents.father , h.hname,h.words from %s/gotCharacters c " +
                "JOIN %s/gotCharacters h ",TEST_INDEX_GAME_OF_THRONES,TEST_INDEX_GAME_OF_THRONES);
        SearchHit[] hits = joinAndGetHits(query);
        Assert.assertEquals(42, hits.length);
    }

    @Test
    public void joinWithGeoIntersectNL() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        String query = String.format("select p1.description,p2.description from %s/location p1 " +
                "JOIN %s/location2 p2 " +
                "ON GEO_INTERSECTS(p2.place,p1.place)",TEST_INDEX_LOCATION,TEST_INDEX_LOCATION2);
        SearchHit[] hits = joinAndGetHits(query);
        Assert.assertEquals(2, hits.length);
        Assert.assertEquals("squareRelated", hits[0].getSourceAsMap().get("p2.description"));
        Assert.assertEquals("squareRelated",hits[1].getSourceAsMap().get("p2.description"));
    }
    @Test
    public void joinWithInQuery() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        String query = String.format("select c.gender ,c.name.firstname, h.hname,h.words from %s/gotCharacters c " +
                "JOIN %s/gotCharacters h on h.hname = c.house" +
                " where c.name.firstname in (select holdersName from %s/dog)", TEST_INDEX_GAME_OF_THRONES, TEST_INDEX_GAME_OF_THRONES, TEST_INDEX_DOG);
        SearchHit[] hits = joinAndGetHits(query);
        Assert.assertEquals(1, hits.length);
        Assert.assertEquals("Daenerys", hits[0].getSourceAsMap().get("c.name.firstname"));
    }



    @Test
    public void joinWithOrHASH() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        joinWithOr(false);
    }
    @Test
    public void joinWithOrNL() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        joinWithOr(true);
    }

    private void joinWithOr(boolean useNestedLoops) throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        String query = String.format("select d.dog_name , c.name.firstname from %s/gotCharacters c " +
                "JOIN %s/dog d on d.holdersName = c.name.firstname" +
                " OR d.age = c.name.ofHisName"
                ,  TEST_INDEX_GAME_OF_THRONES, TEST_INDEX_DOG);
        if(useNestedLoops) query = query.replace("select","select /*! USE_NL*/ ");
        SearchHit[] hits = joinAndGetHits(query);
        Assert.assertEquals(2, hits.length);
        Map<String,Object> oneMatch =  ImmutableMap.of("c.name.firstname", (Object) "Daenerys", "d.dog_name", "rex");
        Map<String,Object> secondMatch =  ImmutableMap.of("c.name.firstname", (Object) "Brandon", "d.dog_name", "snoopy");
        Assert.assertTrue("hits contains daenerys",hitsContains(hits, oneMatch));
        Assert.assertTrue("hits contains brandon",hitsContains(hits, secondMatch));
    }

    @Test
    public void joinWithOrWithTermsFilterOpt() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        String query = String.format("select /*! HASH_WITH_TERMS_FILTER*/ d.dog_name , c.name.firstname from %s/gotCharacters c " +
                "JOIN %s/dog d on d.holdersName = c.name.firstname" +
                " OR d.age = c.name.ofHisName"
                , TEST_INDEX_GAME_OF_THRONES, TEST_INDEX_DOG);

        String explainedQuery = hashJoinRunAndExplain(query);
        boolean containsHoldersNamesTerms = explainedQuery.replaceAll("\\s+","").contains("\"terms\":{\"holdersName\":");
        Assert.assertTrue(containsHoldersNamesTerms);
        List<String> holdersName = Arrays.asList("daenerys","brandon","eddard","jaime");
        for(String holderName : holdersName){
            Assert.assertTrue("should contain:" + holderName , explainedQuery.contains(holderName));
        }
        boolean containsAgesTerms = explainedQuery.replaceAll("\\s+","").contains("\"terms\":{\"age\":");
        Assert.assertTrue(containsAgesTerms);
    }


    @Test
    public void joinWithOrderbyFirstTableHASH() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        joinWithOrderFirstTable(false);
    }
    @Test
    public void joinWithOrderbyFirstTableNL() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        joinWithOrderFirstTable(true);
    }
    private void joinWithOrderFirstTable(boolean useNestedLoops) throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        String query = String.format("select c.name.firstname , d.words from %s/gotCharacters c " +
                "JOIN %s/gotCharacters d on d.hname = c.house " +
                "order by c.name.firstname"
                ,  TEST_INDEX_GAME_OF_THRONES, TEST_INDEX_GAME_OF_THRONES);
        if(useNestedLoops) query = query.replace("select","select /*! USE_NL*/ ");
        SearchHit[] hits = joinAndGetHits(query);
        Assert.assertEquals(16, hits.length);
        Assert.assertEquals("Brandon",hits[0].getSourceAsMap().get("c.name.firstname"));
        Assert.assertEquals("Daenerys",hits[1].getSourceAsMap().get("c.name.firstname"));
        Assert.assertEquals("Eddard",hits[2].getSourceAsMap().get("c.name.firstname"));
        Assert.assertEquals("Jaime",hits[3].getSourceAsMap().get("c.name.firstname"));
    }


    @Test
    public void joinWithAllFromSecondTableHASH() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        joinWithAllFromSecondTable(false);
    }
    @Test
    public void joinWithAllFromSecondTableNL() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        joinWithAllFromSecondTable(true);
    }
    private void joinWithAllFromSecondTable(boolean useNestedLoops) throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        String query = String.format("select c.name.firstname , d.* from %s/gotCharacters c " +
                "JOIN %s/gotCharacters d on d.hname = c.house "
                ,  TEST_INDEX_GAME_OF_THRONES, TEST_INDEX_GAME_OF_THRONES);
        if(useNestedLoops) query = query.replace("select","select /*! USE_NL*/ ");
        SearchHit[] hits = joinAndGetHits(query);
        Assert.assertEquals(16, hits.length);
        //Assert.assertEquals(5,hits[0].getSourceAsMap().size());
    }


    @Test
    public void joinWithAllFromFirstTableHASH() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        joinWithAllFromFirstTable(false);
    }
    @Test
    public void joinWithAllFromFirstTableNL() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        joinWithAllFromFirstTable(true);
    }

    private void joinWithAllFromFirstTable(boolean useNestedLoops) throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        String query = String.format("select  d.* , c.name.firstname from %s/gotCharacters d " +
                "JOIN %s/gotCharacters c  on  c.house = d.hname  "
                ,  TEST_INDEX_GAME_OF_THRONES, TEST_INDEX_GAME_OF_THRONES);
        if(useNestedLoops) query = query.replace("select","select /*! USE_NL*/ ");
        SearchHit[] hits = joinAndGetHits(query);
        Assert.assertEquals(16, hits.length);
        //Assert.assertEquals(5,hits[0].getSourceAsMap().size());
    }

    @Test
    public void leftJoinWithAllFromSecondTableHASH() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        leftJoinWithAllFromSecondTable(false);
    }
    @Test
    public void leftJoinWithAllFromSecondTableNL() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        leftJoinWithAllFromSecondTable(true);
    }
    private void leftJoinWithAllFromSecondTable(boolean useNestedLoops) throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        String query = String.format("select c.name.firstname , d.* from %s/gotCharacters c " +
                "LEFT JOIN %s/gotCharacters d on d.name = c.house " +
                "where d.sigil <> 'direwolf'"
                ,  TEST_INDEX_GAME_OF_THRONES, TEST_INDEX_GAME_OF_THRONES);
        if(useNestedLoops) query = query.replace("select","select /*! USE_NL*/ ");
        SearchHit[] hits = joinAndGetHits(query);
        Assert.assertEquals(10, hits.length);
        for (SearchHit hit : hits) {
            if(hit.getId().endsWith("0")){
                Assert.assertEquals(1,hit.getSourceAsMap().size());
            }
            else {
                Assert.assertEquals(5,hit.getSourceAsMap().size());
            }
        }

    }


    private String hashJoinRunAndExplain(String query) throws IOException, SqlParseException, SQLFeatureNotSupportedException {
        SearchDao searchDao = MainTestSuite.getSearchDao();
        HashJoinElasticRequestBuilder explain = (HashJoinElasticRequestBuilder) searchDao.explain(query).explain();
        HashJoinElasticExecutor executor = new HashJoinElasticExecutor(searchDao.getClient(),  explain);
        executor.run();
        return explain.explain();
    }

    private SearchHit[] joinAndGetHits(String query) throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        SearchDao searchDao = MainTestSuite.getSearchDao();
        SqlElasticRequestBuilder explain = searchDao.explain(query).explain();
        ElasticJoinExecutor executor  = ElasticJoinExecutor.createJoinExecutor(searchDao.getClient(),explain);
        executor.run();
        return executor.getHits().getHits();
    }

    private boolean hitsContains(SearchHit[] hits, Map<String, Object> matchMap) {
        for(SearchHit hit : hits){
            Map<String, Object> hitMap = hit.getSourceAsMap();
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
