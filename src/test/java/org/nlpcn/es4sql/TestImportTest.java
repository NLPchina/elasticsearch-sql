package org.nlpcn.es4sql;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class TestImportTest {
	public static void main(String[] args) throws IOException {
		Client client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("/home/ansj/temp/20140421.txt"))));
		String temp = null;

		while ((temp = br.readLine()) != null) {
			
System.out.println("in");
			try {
				JSONObject job = JSON.parseObject(br.readLine());
				client.prepareIndex().setIndex("testdoc").setType("testdoc").setSource(job).execute().actionGet();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
