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
		SearchResult result = searchDao.selectAsResult("select topHits('from'=0,size=1,age='desc') as hit,sum(age),sum(account_number) from bank where age >30 order by age asc  limit 10 ");
		System.out.println(JSONObject.toJSONString(result));
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
