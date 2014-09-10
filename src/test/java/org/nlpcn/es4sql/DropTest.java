package org.nlpcn.es4sql;

import java.io.IOException;

import org.elasticsearch.action.ActionResponse;
import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;

public class DropTest {
	private SearchDao searchDao = new SearchDao("localhost", 9300);
	@Test
	public void dropIndexTest() throws IOException, SqlParseException {
		ActionResponse select = searchDao.drop("logstash-2014.08.29");
		System.out.println(select);
	}
}
