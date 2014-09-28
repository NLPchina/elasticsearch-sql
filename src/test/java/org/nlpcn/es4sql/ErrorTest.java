package org.nlpcn.es4sql;

import java.io.IOException;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;

public class ErrorTest {

	private SearchDao searchDao = new SearchDao("ky_ESearch", "172.21.19.57", 9300);

	@Test
	public void dayGAndL() throws IOException, SqlParseException {
//		SearchResponse execute = searchDao.execute("select * from adlog where 'programDetail.id'=12536799 limit 10");
		SearchResponse execute = searchDao.execute("select count(distinct clientInfo.clientId) from adlog where programDetail.id=12536799 limit 10");
		
		System.out.println(execute);

	}
}
