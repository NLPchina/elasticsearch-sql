package org.nlpcn.es4sql;

import java.io.IOException;

import org.elasticsearch.action.search.SearchResponse;
import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;

public class QueryTest {
	
	private SearchDao searchDao = new SearchDao("localhost", 9300) ;
	
	@Test
	public void betweenTest() throws IOException, SqlParseException{
		SearchResponse select = searchDao.select("select age from bank where age between 20 and 21 limit 3");
		System.out.println(select);
	}
	
	@Test
	public void notBetweenTest() throws IOException, SqlParseException{
		SearchResponse select = searchDao.select("select age from bank where age not between 20 and 21 limit 3");
		System.out.println(select);
	}
	
	@Test
	public void inTest() throws IOException, SqlParseException{
		SearchResponse select = searchDao.select("select age from bank where age  in (20,21) limit 3");
		System.out.println(select);
	}
	
	@Test
	public void notInTest() throws IOException, SqlParseException{
		SearchResponse select = searchDao.select("select age from bank where age  in (20,21) limit 3");
		System.out.println(select);
	}
	
}
