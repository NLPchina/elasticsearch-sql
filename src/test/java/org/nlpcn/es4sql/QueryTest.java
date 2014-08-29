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
	
	
	@Test
	public void dateSearch() throws IOException, SqlParseException{
		SearchResponse select = searchDao.select("select insert_time from online where insert_time<'2014-08-18' limit 3");
		System.out.println(select);
	}
	
	@Test
	public void dateBetweenSearch() throws IOException, SqlParseException{
		SearchResponse select = searchDao.select("select insert_time from online where insert_time between '2014-08-18' and '2014-08-21' limit 3");
		System.out.println(select);
	}
	
	/**
	 * 是否存在查询
	 * @throws IOException
	 * @throws SqlParseException
	 */
	@Test
	public void missFilterSearch() throws IOException, SqlParseException{
		SearchResponse select = searchDao.select("select insert_time from online where insert_time is not miss order by _score desc limit 10");
		System.out.println(select);
	}
	
	@Test
	public void missQuerySearch() throws IOException, SqlParseException{
		SearchResponse select = searchDao.select("select insert_time from online where insert_time is not miss limit 10");
		System.out.println(select);
	}
}
