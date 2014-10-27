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

	private SearchDao searchDao = new SearchDao("ky_ESearch", "172.21.19.57", 9300);

	@Test
	public void dayGAndL() throws IOException, SqlParseException {
//		SearchResponse execute = searchDao.execute("select * from adlog where 'programDetail.id'=12536799 limit 10");
//		SearchResponse execute = searchDao.execute("select count(distinct clientInfo.clientId) from adlog where programDetail.id=12536799 limit 10");
//		SearchResponse execute = searchDao.execute("select count(*) from heartbeat|2014-10-7");
		
//		System.out.println(execute);
		
//		for (int i = 10; i < 13; i++) {
//		int i=13 ;
//			SearchResponse execute = searchDao.execute("select count(*) from heartbeat/2014-10-"+i+"  "
//					+ "group by date_histogram(field='log_time','interval'='1m','format'='yyyy-MM-dd HH:mm:ss'),tvId");;
//			
//			FileOutputStream fos = new FileOutputStream("/home/ansj/下载/bucket"+i+".json");
//			
//			fos.write(execute.toString().getBytes()) ;
//			fos.flush(); 
//			fos.close();
//		}
		
//		SearchResponse execute = searchDao.execute("select * from heartbeat/2014-10-13 group by vendorId");
//		
//		System.out.println(execute);
		
//		SearchResponse execute = searchDao.execute("select count(*) from heartbeat "
//				+ " group by date_histogram(field='log_time','interval'='1H','format'='yyyy-MM-dd HH:mm:ss'),"
//				+ "range(tvId,0,1,10000)");
		
//		SearchResponse execute = searchDao.execute("SELECT count(certificateCode) FROM heartbeat/2014-10-16") ;
//		
//		System.out.println(execute);machineType
//		System.out.println(searchDao.explan("select count(clientInfo.clientId), count(distinct clientInfo.clientId) from adlog where invoke_date_int=20141017 and adDetailInfo.advertiserId=4   group by invoke_date_int"));
//		SearchResponse execute = searchDao.execute("select count(clientInfo.clientId), count(distinct clientInfo.clientId) from adlog where invoke_date_int=20141017 and adDetailInfo.advertiserId=4   group by invoke_date_int") ;
//		
//		
//		System.out.println(execute);
		SearchResponse execute = searchDao.execute("select count(distinct certificateCode) as total from heartbeat/2014-10-26 group by vendorId, machineType") ;
		
		System.out.println(execute);
		
//		System.out.println(searchDao.explan("SELECT * FROM data-2014.10.124,data-2014.10.124/dsf, 342 123dsfs/sdfdsf where a >3"));
		
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
