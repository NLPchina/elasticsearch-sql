package org.elasticsearch.plugin.nlpcn.executors;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.rest.RestChannel;
import org.nlpcn.es4sql.query.QueryAction;

import java.util.Map;

/**
 * Created by Eliran on 26/12/2015.
 */
public interface RestExecutor {
    void execute(Client client, Map<String, String> params, QueryAction queryAction, RestChannel channel) throws Exception;

    String execute(Client client, Map<String, String> params, QueryAction queryAction) throws Exception;
}
