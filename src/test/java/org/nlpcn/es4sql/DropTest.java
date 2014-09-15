package org.nlpcn.es4sql;

import java.io.IOException;

import org.elasticsearch.action.ActionResponse;
import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;

public class DropTest {
	private SearchDao searchDao = new SearchDao("172.21.19.210", 9300);
	@Test
	public void dropIndexTest() throws IOException, SqlParseException {
		ActionResponse select = searchDao.drop("identify.log_2014-08-*");
		System.out.println(select);
	}
}
