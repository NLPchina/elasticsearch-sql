package org.nlpcn.es4sql;

import java.io.IOException;

import org.elasticsearch.action.search.SearchResponse;
import org.junit.Test;
import org.nlpcn.es4sql.domain.SearchResult;
import org.nlpcn.es4sql.exception.SqlParseException;

import com.alibaba.fastjson.JSONObject;

public class AgggationResultTest {
	
	private SearchDao searchDao = new SearchDao("localhost", 9300) ;
	
	@Test
	public void sumTest() throws IOException, SqlParseException{
		try {
			SearchResponse select = searchDao.select("select sum(age),count(*), count(distinct age) from bank  group by gender order by count(distinct age)  desc  limit 1");
			System.out.println(select);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void sumSortCount() throws IOException, SqlParseException{
		try {
			SearchResponse select = searchDao.select("select sum(age),count(*) as kk, count(age) as k from bank  group by gender order by kk asc limit 1 ");
			System.out.println(select);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	@Test
	public void orderByAagSortTest() throws IOException, SqlParseException{
		SearchResponse result = searchDao.select("select sum(age),count(*),count(account_number)  from bank  group by age,account_number ");
		System.out.println(result);
	}
	
	@Test
	public void minTest() throws IOException, SqlParseException{
		SearchResult result = searchDao.selectAsResult("select min(age),sum(account_number) from bank group by gender limit 10");
		System.out.println(result);
	}
}
