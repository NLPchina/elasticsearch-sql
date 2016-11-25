package org.elasticsearch.plugin.nlpcn;

import java.io.IOException;

import org.elasticsearch.search.SearchHits;
import org.nlpcn.es4sql.exception.SqlParseException;

/**
 * Created by Eliran on 21/8/2016.
 */
public interface ElasticHitsExecutor {
    public void run() throws IOException, SqlParseException ;
    public SearchHits getHits();
}
