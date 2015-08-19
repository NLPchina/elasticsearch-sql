package org.nlpcn.es4sql;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHits;
import org.junit.Assert;
import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.SqlElasticRequestBuilder;
import org.nlpcn.es4sql.query.SqlElasticSearchRequestBuilder;
import org.nlpcn.es4sql.query.explain.ExplainManager;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLFeatureNotSupportedException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX;

/**
 * Created by Eliran on 15/8/2015.
 */
public class JoinTest {
    @Test
    public void leftJoinTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        String query = "SELECT * FROM elasticsearch-sql_test_index/account LEFT JOIN elasticsearch-sql_test_index/dog on dog.holdersName = account.firstname";
        //SearchHits response = query(query);
        //        Assert.assertEquals(1004, response.getTotalHits());

        String explain = explain(query);
//        System.out.println("explain = " + explain);

    }

    @Test
    public void straightJoinTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        String query = "SELECT * FROM elasticsearch-sql_test_index/account JOIN elasticsearch-sql_test_index/dog on dog.holdersName = account.firstname";
        //SearchHits response = query(query);
        //        Assert.assertEquals(1004, response.getTotalHits());

        String explain = explain(query);
//        System.out.println("explain = " + explain);

    }

    private SearchHits query(String query) throws SqlParseException, SQLFeatureNotSupportedException, SQLFeatureNotSupportedException {
        SearchDao searchDao = MainTestSuite.getSearchDao();
        SqlElasticSearchRequestBuilder select = (SqlElasticSearchRequestBuilder) searchDao.explain(query);
        return ((SearchResponse)select.get()).getHits();
    }

    private String explain(String sql) throws SQLFeatureNotSupportedException, SqlParseException, InvocationTargetException, NoSuchMethodException, IllegalAccessException, IOException {
        SearchDao searchDao = MainTestSuite.getSearchDao();
        SqlElasticRequestBuilder requestBuilder = searchDao.explain(sql);
        return requestBuilder.explain();
    }

}
