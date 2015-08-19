package org.nlpcn.es4sql;

import java.io.IOException;
import java.sql.SQLFeatureNotSupportedException;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.SqlElasticSearchRequestBuilder;

/**
 * some bad case 
 * @author ansj
 *
 */
public class BugTest {

	
	@Test
	public void bug1() throws IOException, SqlParseException, SQLFeatureNotSupportedException {

        SqlElasticSearchRequestBuilder select = (SqlElasticSearchRequestBuilder) MainTestSuite.getSearchDao().explain("select count(*),sum(age) from bank");
		System.out.println(select);
	}
}
