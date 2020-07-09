package org.elasticsearch.plugin.nlpcn.executors;

import com.google.common.base.Joiner;
import org.elasticsearch.client.Client;
import org.elasticsearch.plugin.nlpcn.QueryActionElasticExecutor;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestStatus;
import org.nlpcn.es4sql.Util;
import org.nlpcn.es4sql.query.QueryAction;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by Eliran on 26/12/2015.
 */
public class CSVResultRestExecutor implements RestExecutor {

    @Override
    public void execute(Client client, Map<String, String> params, QueryAction queryAction, RestChannel channel) throws Exception {
        Object queryResult = QueryActionElasticExecutor.executeAnyAction(client, queryAction);

        boolean flat = getBooleanOrDefault(params,"flat",false);
        String separator = ",";
        if(params.containsKey("separator")){
         separator = params.get("separator");
        }
        boolean includeScore = getBooleanOrDefault(params,"_score",false);
        boolean includeType = getBooleanOrDefault(params,"_type",false);
        boolean includeId = getBooleanOrDefault(params,"_id",false);
        boolean includeScrollId = getBooleanOrDefault(params,"_scroll_id",false);
        boolean quote = getBooleanOrDefault(params, "quote", false);
        CSVResult result  = new CSVResultsExtractor(includeScore,includeType,includeId,includeScrollId,queryAction).extractResults(queryResult,flat,separator,quote);
        String newLine = "\n";
        if(params.containsKey("newLine")){
         newLine = params.get("newLine");
        }
        boolean showHeader = getBooleanOrDefault(params, "showHeader", true);
        String csvString = buildString(separator, result, newLine, showHeader, quote);
        BytesRestResponse bytesRestResponse = new BytesRestResponse(RestStatus.OK, csvString);
        channel.sendResponse(bytesRestResponse);
    }

    @Override
    public String execute(Client client, Map<String, String> params, QueryAction queryAction) throws Exception {
        Object queryResult = QueryActionElasticExecutor.executeAnyAction(client, queryAction);

        boolean flat = getBooleanOrDefault(params,"flat",false);
        String separator = ",";
        if(params.containsKey("separator")){
            separator = params.get("separator");
        }
        boolean includeScore = getBooleanOrDefault(params,"_score",false);
        boolean includeType = getBooleanOrDefault(params,"_type",false);
        boolean includeId = getBooleanOrDefault(params,"_id",false);
        boolean includeScrollId = getBooleanOrDefault(params,"_scroll_id",false);
        boolean quote = getBooleanOrDefault(params, "quote", false);
        CSVResult result  = new CSVResultsExtractor(includeScore,includeType,includeId,includeScrollId,queryAction).extractResults(queryResult,flat,separator,quote);
        String newLine = "\n";
        if(params.containsKey("newLine")){
            newLine = params.get("newLine");
        }
        boolean showHeader = getBooleanOrDefault(params, "showHeader", true);
        String csvString = buildString(separator, result, newLine, showHeader, quote);
        return csvString;
    }

    private boolean getBooleanOrDefault(Map<String, String> params, String param, boolean defaultValue) {
        boolean flat = defaultValue;
        if(params.containsKey(param)){
            flat = Boolean.parseBoolean(params.get(param));
        }
        return flat;
    }

    private String buildString(String separator, CSVResult result, String newLine, boolean showHeader, boolean quote) {
        StringBuilder csv = new StringBuilder();
        if (showHeader) {
            csv.append(Joiner.on(separator).join(quote ? result.getHeaders().stream().map(Util::quoteString).collect(Collectors.toList()) : result.getHeaders()));
            csv.append(newLine);
        }
        csv.append(Joiner.on(newLine).join(result.getLines()));
        return csv.toString();
    }

}
