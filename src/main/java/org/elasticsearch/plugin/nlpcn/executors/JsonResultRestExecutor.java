package org.elasticsearch.plugin.nlpcn.executors;

import java.util.List;
import java.util.Map;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugin.nlpcn.QueryActionElasticExecutor;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestStatus;
import org.nlpcn.es4sql.query.QueryAction;

/**
 * 
 * @author shuzhangyao@163.com 2016年11月8日 上午10:54:10
 *
 */
public class JsonResultRestExecutor implements RestExecutor {

	@Override
	public void execute(Client client, Map<String, String> params, QueryAction queryAction, RestChannel channel) throws Exception {
		Object queryResult = QueryActionElasticExecutor.executeAnyAction(client, queryAction);

		boolean flat = getBooleanOrDefault(params, "flat", false);
		boolean includeScore = getBooleanOrDefault(params, "_score", false);
		boolean includeType = getBooleanOrDefault(params, "_type", false);
		boolean includeId = getBooleanOrDefault(params, "_id", false);
		CommonResult result = new CommonResultsExtractor(includeScore, includeType, includeId).extractResults(queryResult, flat);
		String jsonString = buildJsonString(result);
		BytesRestResponse bytesRestResponse = new BytesRestResponse(RestStatus.OK, jsonString);
		channel.sendResponse(bytesRestResponse);
	}

	@Override
	public String execute(Client client, Map<String, String> params, QueryAction queryAction) throws Exception {
		Object queryResult = QueryActionElasticExecutor.executeAnyAction(client, queryAction);

		boolean flat = getBooleanOrDefault(params, "flat", false);
		boolean includeScore = getBooleanOrDefault(params, "_score", false);
		boolean includeType = getBooleanOrDefault(params, "_type", false);
		boolean includeId = getBooleanOrDefault(params, "_id", false);
		CommonResult result = new CommonResultsExtractor(includeScore, includeType, includeId).extractResults(queryResult, flat);
		String csvString = buildJsonString(result);
		return csvString;
	}

	private boolean getBooleanOrDefault(Map<String, String> params, String param, boolean defaultValue) {
		boolean flat = defaultValue;
		if (params.containsKey(param)) {
			flat = Boolean.parseBoolean(params.get(param));
		}
		return flat;
	}

	private String buildJsonString(CommonResult result) {
		String jsonString = "";
		try {
			XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON).prettyPrint();
			builder.startArray();
			List<String> keys = result.getKeys();
			for (List<Object> line : result.getValues()) {
				int i = 0;
				builder.startObject();
				for (Object value : line) {
					builder.field(keys.get(i), value);
					i++;
				}
				builder.endObject();
			}
			builder.endArray();
			jsonString = builder.string();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return jsonString;
	}
}
