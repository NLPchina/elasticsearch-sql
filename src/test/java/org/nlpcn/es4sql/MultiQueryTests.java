package org.nlpcn.es4sql;

import org.elasticsearch.plugin.nlpcn.ElasticHitsExecutor;
import org.elasticsearch.plugin.nlpcn.ElasticJoinExecutor;
import org.elasticsearch.plugin.nlpcn.MultiRequestExecutorFactory;
import org.elasticsearch.search.SearchHit;
import org.junit.Assert;
import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.SqlElasticRequestBuilder;

import java.io.IOException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashSet;
import java.util.Set;

import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX;

/**
 * Created by Eliran on 21/8/2016.
 */
public class MultiQueryTests {

    @Test
    public void unionAllSameRequestOnlyOneRecordTwice() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        String query = String.format("SELECT firstname FROM %s/account WHERE firstname = 'Amber' limit 1 union all SELECT firstname FROM %s/account WHERE firstname = 'Amber'",TEST_INDEX,TEST_INDEX);
        SearchHit[] searchHits = executeAndGetHits(query);
        Assert.assertEquals(2,searchHits.length);
        for(SearchHit hit : searchHits) {
            Object firstname = hit.sourceAsMap().get("firstname");
            Assert.assertEquals("Amber",firstname);
        }
    }

    @Test
    public void unionAllOnlyOneRecordEachWithAlias() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        String query = String.format("SELECT firstname FROM %s/account WHERE firstname = 'Amber' " +
                "union all " +
                "SELECT dog_name as firstname FROM %s/dog WHERE dog_name = 'rex'",TEST_INDEX,TEST_INDEX);
        SearchHit[] searchHits = executeAndGetHits(query);
        Assert.assertEquals(2,searchHits.length);
        Set<String> names = new HashSet<>();
        for(SearchHit hit : searchHits) {
            Object firstname = hit.sourceAsMap().get("firstname");
            names.add(firstname.toString());
        }
        Assert.assertTrue("names should contain Amber",names.contains("Amber"));
        Assert.assertTrue("names should contain rex",names.contains("rex"));
    }

    @Test
    public void unionAllOnlyOneRecordEachWithComplexAlias() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        String query = String.format("SELECT firstname FROM %s/account WHERE firstname = 'Amber' " +
                "union all " +
                "SELECT name.firstname as firstname FROM %s/gotCharacters WHERE name.firstname = 'Daenerys'",TEST_INDEX,TEST_INDEX);
        SearchHit[] searchHits = executeAndGetHits(query);
        Assert.assertEquals(2,searchHits.length);
        Set<String> names = new HashSet<>();
        for(SearchHit hit : searchHits) {
            Object firstname = hit.sourceAsMap().get("firstname");
            names.add(firstname.toString());
        }
        Assert.assertTrue("names should contain Amber",names.contains("Amber"));
        Assert.assertTrue("names should contain Daenerys",names.contains("Daenerys"));
    }
    private SearchHit[] executeAndGetHits(String query) throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        SearchDao searchDao = MainTestSuite.getSearchDao();
        SqlElasticRequestBuilder explain = searchDao.explain(query).explain();
        ElasticHitsExecutor executor  = MultiRequestExecutorFactory.createExecutor(searchDao.getClient(),(org.nlpcn.es4sql.query.multi.MultiQueryRequestBuilder) explain);
        executor.run();
        return executor.getHits().getHits();
    }

}
