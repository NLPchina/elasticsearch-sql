package org.nlpcn.es4sql;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.nlpcn.commons.lang.util.FileFinder;

import com.alibaba.fastjson.JSONObject;

@SuppressWarnings(value = { "all" })
public class ImportTestData {
	public static void main(String[] args) throws ElasticsearchException, IOException, ParseException {
		Client client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(FileFinder.find("accounts.json"))));
		String temp = null;

		while ((temp = br.readLine()) != null) {
			JSONObject id = JSONObject.parseObject(temp);
			JSONObject job = JSONObject.parseObject(br.readLine());

			try {
				client.prepareIndex().setIndex("bank").setType("accounts").setId(id.getJSONObject("index").getString("_id")).setSource(job).execute().actionGet();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		br = new BufferedReader(new InputStreamReader(new FileInputStream(FileFinder.find("online_info.json"))));
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		while ((temp = br.readLine()) != null) {
			JSONObject job = JSONObject.parseObject(temp);

			String time = ((JSONObject) job.get("insert_time")).getString("$date");

			Date date = new Date(format.parse(time).getTime() + 8 * 3600 * 1000L);

			job.put("insert_time", date);
			job.put("date", date.getDate());
			job.put("hours", date.getHours());
			job.put("date_hours", date.getDate() + "_" + date.getHours());

			try {
				client.prepareIndex().setIndex("online").setType("online").setId(job.remove("_id").toString()).setSource(job).execute().actionGet();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
