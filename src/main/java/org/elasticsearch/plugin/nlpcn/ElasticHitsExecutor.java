package org.elasticsearch.plugin.nlpcn;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.search.SearchHits;
import org.nlpcn.es4sql.domain.Select;
import org.nlpcn.es4sql.exception.SqlParseException;

import java.io.IOException;

/**
 * Created by Eliran on 21/8/2016.
 */
public interface ElasticHitsExecutor {
    public void run() throws IOException, SqlParseException ;
    public SearchHits getHits();
}
