package org.nlpcn.es4sql;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.nlpcn.es4sql.exception.SqlParseException;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class Test {
	public static void main(String[] args) throws IOException, SqlParseException, ParseException {
		SearchDao searchDao = new SearchDao("ky_ESearch", "172.21.19.57", 9300);
		String sql = "SELECT COUNT(DISTINCT path) FROM adlog group by path limit 3 order by COUNT(DISTINCT path)";
		ActionResponse select = searchDao.execute(sql);
		System.out.println(select);
	}

	public static void importData() throws IOException {
		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "test").build();
		Client client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress("192.168.200.19", 9300));
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("/home/ansj/workspace-ee/log-analysis/user_feature.json"))));
		String temp = null;
		int i = 0;
		while ((temp = br.readLine()) != null) {
			try {
				JSONObject job = JSON.parseObject(temp);
				System.out.println(++i);
				client.prepareIndex().setIndex("user_feature").setType("user_feature").setId(job.remove("_id").toString()).setSource(job).execute().actionGet();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
