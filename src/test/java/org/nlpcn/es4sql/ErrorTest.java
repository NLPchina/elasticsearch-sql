package org.nlpcn.es4sql;

import java.io.IOException;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;

public class ErrorTest {
	
	private SearchDao searchDao = new SearchDao("localhost", 9300);
	@Test
	public void dayGAndL() throws IOException, SqlParseException {
		SearchResponse execute = searchDao.execute(" select certificateCode,vendorId,machineType,certificateCode,status,ip,time_milli from heartbeat where time_milli>=1411643933461 and time_milli <= 1411644139653 order by time_milli asc") ;
		
		System.out.println(execute);
	}
}
