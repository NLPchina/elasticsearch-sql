package org.nlpcn.es4sql;

import java.io.IOException;

import org.elasticsearch.action.search.SearchResponse;
import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;

public class AgggationTest {
	
	private SearchDao searchDao = new SearchDao("localhost", 9300) ;
	
	@Test
	public void sumTest() throws IOException, SqlParseException{
		SearchResponse select = searchDao.select("select sum(age),sum(account_number) from bank where age >30 group by gender order by age asc  limit 10 ");
		System.out.println(select);
	}
	
	@Test
	public void maxTest() throws IOException, SqlParseException{
		SearchResponse select = searchDao.select("select max(age),sum(account_number) from bank group by gender limit 10");
		System.out.println(select);
	}
	
	@Test
	public void minTest() throws IOException, SqlParseException{
		SearchResponse select = searchDao.select("select min(age),sum(account_number) from bank group by gender limit 10");
		System.out.println(select);
	}
}
