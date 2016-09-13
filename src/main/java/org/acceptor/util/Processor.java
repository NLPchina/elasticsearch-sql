package org.acceptor.util;

import java.sql.SQLFeatureNotSupportedException;

import org.nlpcn.es4sql.exception.SqlParseException;

import com.alibaba.fastjson.JSONObject;

import io.netty.channel.ChannelHandlerContext;

public class Processor implements Runnable {
	
	private ChannelHandlerContext ctx = null;
	private String msg = null;
	
	public Processor(ChannelHandlerContext ctx, String msg) {
		this.ctx = ctx;
		this.msg = msg;
	}
	
	public void processRequest() {
		JSONObject json = JSONObject.parseObject(this.msg);
		String cluster = json.getString("cluster");
		String ip = json.getString("ip");
		String cmd = json.getString("cmd");
		String sql = json.getString("sql");
		String result = null;
		if("explain".equals(cmd)) {
			try {
				result = EsUtil.explain(sql, cluster, ip);
			} catch (SQLFeatureNotSupportedException | SqlParseException e) {
				e.printStackTrace();
				result = e.getMessage();
			}
		} else if("query".equals(cmd)) {
			try {
				result = EsUtil.execGet(sql, cluster, ip);
			} catch (SQLFeatureNotSupportedException | SqlParseException e) {
				e.printStackTrace();
				result = e.getMessage();
			}
		} else {
			result = "Unsupport Cmd [" + cmd + "]";
		}
		this.ctx.writeAndFlush(result);
	}

	@Override
	public void run() {
		this.processRequest();
	}
}
