package org.nlpcn.es4sql;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHits;
import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.SqlElasticRequestBuilder;
import org.nlpcn.es4sql.query.SqlElasticSearchRequestBuilder;

import java.sql.SQLFeatureNotSupportedException;

/**
 * Created by Eliran on 22/8/2015.
 */
public class JoinTests {


    @Test
    public void joinParseCheckSelectedFieldsSplit() throws SqlParseException, SQLFeatureNotSupportedException {
        String query = "SELECT a.firstname ,a.lastname , a.gender ,  d.holdersName ,d.name  FROM elasticsearch-sql_test_index/account a " +
                "LEFT JOIN elasticsearch-sql_test_index/dog d on d.holdersName = a.firstname " +
                " AND d.age < a.age " +
                " WHERE a.firstname = 'eliran' AND " +
                " (a.age > 10 OR a.balance > 2000)" +
                " AND d.age > 1";
        SearchDao searchDao = MainTestSuite.getSearchDao();
        SqlElasticRequestBuilder explain = searchDao.explain(query);
    }

}
