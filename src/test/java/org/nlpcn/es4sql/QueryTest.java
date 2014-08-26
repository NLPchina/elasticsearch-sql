package org.nlpcn.es4sql;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.search.SearchResponse;
import org.junit.Test;
import org.nlpcn.es4sql.domain.SearchResult;
import org.nlpcn.es4sql.exception.SqlParseException;

import com.alibaba.fastjson.JSONObject;

public class QueryTest {
	
	private SearchDao searchDao = new SearchDao("localhost", 9300) ;
	
	@Test
	public void sumTest() throws IOException, SqlParseException{
		SearchResult select = searchDao.selectAsResult("select * from bank ");
		
		Set<String> sets = new HashSet<>() ;
		
		 List<Map<String, Object>> results = select.getResults() ;
		
		for (Map<String, Object> map : results) {
			System.out.println(map);
			sets.add(map.get("city").toString());
		}
		
		System.out.println(sets.size());
		System.out.println(sets);
		
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
