package org.nlpcn.es4sql;

import java.io.IOException;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;

/**
 * some bad case 
 * @author ansj
 *
 */
public class BugTest {

	
	@Test
	public void bug1() throws IOException, SqlParseException{

		SearchRequestBuilder select = (SearchRequestBuilder) MainTestSuite.getSearchDao().explain("select count(*),sum(age) from bank");
		System.out.println(select);
	}
}
