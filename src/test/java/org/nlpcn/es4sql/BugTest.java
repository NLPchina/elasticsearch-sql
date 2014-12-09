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

	private SearchDao searchDao = new SearchDao();
	
	@Test
	public void bug1() throws IOException, SqlParseException{

		SearchRequestBuilder select = searchDao.explan("select count(*),sum(age) from bank");
		System.out.println(select);
	}
}
