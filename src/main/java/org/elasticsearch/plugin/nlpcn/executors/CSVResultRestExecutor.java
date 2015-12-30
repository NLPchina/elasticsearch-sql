package org.elasticsearch.plugin.nlpcn.executors;

import com.google.common.base.Joiner;
import org.elasticsearch.client.Client;
import org.elasticsearch.plugin.nlpcn.QueryActionElasticExecutor;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestStatus;
import org.nlpcn.es4sql.query.QueryAction;

import java.util.*;

/**
 * Created by Eliran on 26/12/2015.
 */
public class CSVResultRestExecutor implements RestExecutor {

    @Override
    public void execute(Client client, Map<String, String> params, QueryAction queryAction, RestChannel channel) throws Exception {
        Object queryResult = QueryActionElasticExecutor.executeAnyAction(client, queryAction);
        boolean flat = false;
        if(params.containsKey("flat")){
            flat = Boolean.parseBoolean(params.get("flat"));
        }
        String separator = ",";
        if(params.containsKey("separator")){
         separator = params.get("separator");
        }
        CSVResult result  = new CSVResultsExtractor().extractResults(queryResult,flat,separator);
        String newLine = "\n";
        if(params.containsKey("newLine")){
         newLine = params.get("newLine");
        }
        String csvString = buildString(separator, result, newLine);
        BytesRestResponse bytesRestResponse = new BytesRestResponse(RestStatus.OK, csvString);
        channel.sendResponse(bytesRestResponse);
    }

    private String buildString(String separator, CSVResult result, String newLine) {
        StringBuilder csv = new StringBuilder();
        csv.append(Joiner.on(separator).join(result.getHeaders()));
        csv.append(newLine);
        csv.append(Joiner.on(newLine).join(result.getLines()));
        return csv.toString();
    }

}
