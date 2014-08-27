package org.nlpcn.es4sql;

import java.io.IOException;

import org.elasticsearch.action.search.SearchResponse;
import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class AgggationTest {
	
	private SearchDao searchDao = new SearchDao("localhost", 9300) ;
	
	@Test
	public void sumTest() throws IOException, SqlParseException{
		SearchResponse select = searchDao.select("select sum(age),sum(account_number) from bank group by city order by age asc  limit 100 ");
		System.out.println(select);
		JSONObject parseObject = JSONObject.parseObject(select.toString()).getJSONObject("aggregations");
		
		JSONArray ja = null ;
		
		if(parseObject.containsKey("filter")){
			parseObject = parseObject.getJSONObject("filter") ;
		}
		
		if(parseObject.containsKey("group by")){
			parseObject = parseObject.getJSONObject("group by") ;
			ja = parseObject.getJSONArray("buckets") ;
		}else{
			ja = new JSONArray() ;
			ja.add(parseObject) ;
		}
		
		System.out.println(ja);
		
		System.out.println(ja.size());
		
		System.out.println();
		
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
