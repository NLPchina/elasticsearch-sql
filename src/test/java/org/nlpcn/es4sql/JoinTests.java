package org.nlpcn.es4sql;


import org.elasticsearch.action.search.SearchResponse;
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

/**
 * Created by Eliran on 22/8/2015.
 */
public class JoinTests {


    @Test
    public void joinParseCheckSelectedFieldsSplit() throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = "SELECT a.firstname ,a.lastname , a.gender ,d.name  FROM elasticsearch-sql_test_index/people a " +
                "LEFT JOIN elasticsearch-sql_test_index/dog d on d.holdersName = a.firstname " +
                " WHERE " +
                " (a.age > 10 OR a.balance > 2000)" +
                " AND d.age > 1";
        SearchDao searchDao = MainTestSuite.getSearchDao();
        SqlElasticRequestBuilder explain = searchDao.explain(query);
        HashJoinElasticExecutor executor = new HashJoinElasticExecutor(searchDao.getClient(), (HashJoinElasticRequestBuilder) explain);
        executor.run();
        SearchHit[] hits = executor.getHits().getHits();
        Assert.assertEquals(2,hits.length);
        Map<String,Object> oneMatch = new HashMap<>();
        oneMatch.put("a.firstname","Daenerys");
        oneMatch.put("a.lastname","Targaryen");
        oneMatch.put("a.gender","M");
        oneMatch.put("d.name", "rex");
        Map<String,Object> secondMatch = new HashMap<>();
        secondMatch.put("a.firstname","Hattie");
        secondMatch.put("a.lastname","Bond");
        secondMatch.put("a.gender","M");
        secondMatch.put("d.name","snoopy");

        Assert.assertTrue(hitsContains(hits, oneMatch));
        Assert.assertTrue(hitsContains(hits,secondMatch));

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
                if(!matchMap.get(entry.getKey()).equals(entry.getValue())){
                    matchedHit = false;
                    break;
                }
            }
            if(matchedHit) return true;
        }
        return false;
    }

}
