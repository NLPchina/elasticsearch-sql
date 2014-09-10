package org.nlpcn.es4sql;

import java.io.IOException;
import java.util.Date;

import org.elasticsearch.action.ActionResponse;
import org.nlpcn.es4sql.exception.SqlParseException;

public class OnlineStaic {
	public static void main(String[] args) throws IOException, SqlParseException {
		SearchDao searchDao = new SearchDao("localhost", 9300);
		ActionResponse select = searchDao.execute("select avg(all_client),avg(all_tv_clinet) from online group by hours limit 100") ;
		
		System.out.println(select);
		
		System.out.println(new Date(1408322887421L));
	}
}
