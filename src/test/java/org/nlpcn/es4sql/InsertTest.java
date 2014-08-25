package org.nlpcn.es4sql;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.nlpcn.commons.lang.util.FileFinder;

import com.alibaba.fastjson.JSONObject;

public class InsertTest {
	public static void main(String[] args) throws ElasticsearchException, IOException {
		Client client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("127.0.0.1", 9300));
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(FileFinder.find("accounts.json"))));
		String temp = null;
		int i = 0;
		while ((temp = br.readLine()) != null) {
			JSONObject job = JSONObject.parseObject(temp) ;
			if (job.containsKey("_id")) {
			    continue;
			}
			try {
				client.prepareIndex().setIndex("bank").setType("accounts").setId(job.getString("account_number")).setSource(job).execute().actionGet();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}
}
