package org.nlpcn.es4sql;

import java.io.IOException;

import org.elasticsearch.action.search.SearchResponse;
import org.junit.Test;
import org.nlpcn.es4sql.domain.SearchResult;
import org.nlpcn.es4sql.exception.SqlParseException;

public class AgggationResultTest {
	
	private SearchDao searchDao = new SearchDao("localhost", 9300) ;
	
	@Test
	public void sumTest() throws IOException, SqlParseException{
		SearchResult result = searchDao.selectAsResult("select sum(age),sum(account_number) from bank where age >30 group by gender order by age asc  limit 10 ");
		System.out.println(result);
	}
	
	@Test
	public void maxTest() throws IOException, SqlParseException{
		SearchResult result = searchDao.selectAsResult("select max(age),sum(account_number) from bank group by gender limit 10");
		System.out.println(result);
	}
	
	@Test
	public void minTest() throws IOException, SqlParseException{
		SearchResult result = searchDao.selectAsResult("select min(age),sum(account_number) from bank group by gender limit 10");
		System.out.println(result);
	}
}
