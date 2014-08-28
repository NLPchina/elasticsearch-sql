package org.nlpcn.es4sql;

import java.io.IOException;

import org.elasticsearch.action.search.SearchResponse;
import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;

public class AggregationTest {

	private SearchDao searchDao = new SearchDao("localhost", 9300);

	@Test
	public void sumTest() throws IOException, SqlParseException {
		try {
			SearchResponse select = searchDao.select("select sum(age),count(*), count(distinct age) from bank  group by gender order by count(distinct age)  desc  limit 1");
			System.out.println(select);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void sumSortCount() throws IOException, SqlParseException {
		SearchResponse select = searchDao.select("select sum(age),count(*) as kk, count(age) as k from bank  group by gender order by kk asc limit 1 ");
		System.out.println(select);
	}

	@Test
	public void minTest() throws IOException, SqlParseException {
		SearchResponse result = searchDao.select("select min(age) from bank  group by gender ");
		System.out.println(result);
	}
	
	@Test
	public void maxTest() throws IOException, SqlParseException {
		SearchResponse result = searchDao.select("select max(age) from bank  group by gender ");
		System.out.println(result);
	}
	
	@Test
	public void avgTest() throws IOException, SqlParseException {
		SearchResponse result = searchDao.select("select avg(age) from bank  group by gender ");
		System.out.println(result);
	}
	
	@Test
	public void countTest() throws IOException, SqlParseException {
		SearchResponse result = searchDao.select("select count(*) from bank  group by gender ");
		System.out.println(result);
	}

}
