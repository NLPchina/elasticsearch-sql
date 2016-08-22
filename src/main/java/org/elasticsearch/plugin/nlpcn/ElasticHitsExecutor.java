package org.elasticsearch.plugin.nlpcn;

import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.search.SearchHits;
import org.nlpcn.es4sql.exception.SqlParseException;

import java.io.IOException;

/**
 * Created by Eliran on 21/8/2016.
 */
public interface ElasticHitsExecutor {
    public void run() throws IOException, SqlParseException ;
    public SearchHits getHits();
}
