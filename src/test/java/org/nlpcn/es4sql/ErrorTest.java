package org.nlpcn.es4sql;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class ErrorTest {

	private SearchDao searchDao = new SearchDao("ky_ESearch", "172.21.19.59", 9300);

	@Test
	public void dayGAndL() throws IOException, SqlParseException {
//		SearchResponse execute = searchDao.execute("select * from adlog where 'programDetail.id'=12536799 limit 10");
//		SearchResponse execute = searchDao.execute("select count(distinct clientInfo.clientId) from adlog where programDetail.id=12536799 limit 10");
//		SearchResponse execute = searchDao.execute("select count(*) from heartbeat|2014-10-7");
//		
//		System.out.println(execute);
//		
//		for (int i = 10; i < 13; i++) {
//			SearchResponse execute = searchDao.execute("select count(*) from heartbeat/2014-10-"+i+" where tvId <> 0 "
//					+ "group by date_histogram(field='log_time','interval'='1H','format'='yyyy-MM-dd HH:mm:ss'),tvId");;
//			
//			FileOutputStream fos = new FileOutputStream("/home/ansj/下载/bucket"+i+".json");
//			
//			fos.write(execute.toString().getBytes()) ;
//			fos.flush(); 
//			fos.close();
//		}
		 
		
		
		System.out.println(searchDao.explan("select rci, sum(costAmountOut) as cost, sum(billedamount) as billedamt, max(datadate) as maxdate, max(id) as maxid from data20141011 where rci <> 0 and rci <> 1538 and rci <> 1624 group by rci order by rci "));
		
//		SearchResponse execute = searchDao.execute("select count(*) from heartbeat/2014-10-10,heartbeat/2014-10-11  where log_time between '2014-10-09T16:00:00.000Z' and '2014-10-10T23:59:59.999Z'");;
//		
//		System.out.println(searchDao.execute("select count(*) from heartbeat/2014-10-10"));
//		
//		System.out.println(execute);
		
		
//		SearchResponse execute = searchDao.execute("select * from heartbeat/2014-10-10,heartbeat/2014-10-11 where tvId=201236592 limit 10");;
//		
//		System.out.println(execute);
	}
}
