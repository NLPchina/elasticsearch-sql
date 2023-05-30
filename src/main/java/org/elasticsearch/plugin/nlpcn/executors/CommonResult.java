package org.elasticsearch.plugin.nlpcn.executors;

import java.util.List;
import java.util.Map;

/**
 * 
 * @author shuzhangyao@163.com 2016年11月8日 上午9:54:22
 *
 *         the #CommonResult can be changed into csv,json ...
 */
public class CommonResult {
	private final List<String> keys;
	private final List<List<Object>> values;

	public CommonResult(List<String> keys, List<List<Object>> values) {
		this.keys = keys;
		this.values = values;
	}

	public List<String> getKeys() {
		return keys;
	}

	public List<List<Object>> getValues() {
		return values;
	}
}
