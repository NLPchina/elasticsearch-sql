package org.elasticsearch.plugin.nlpcn;

import org.elasticsearch.search.SearchHits;
import org.nlpcn.es4sql.exception.SqlParseException;

import java.io.IOException;

/**
 * Created by Eliran on 21/8/2016.
 */
public interface ElasticHitsExecutor {
    void run() throws IOException, SqlParseException ;
    SearchHits getHits();
}
