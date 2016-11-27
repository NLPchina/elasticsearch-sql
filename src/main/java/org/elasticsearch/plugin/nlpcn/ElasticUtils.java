package org.elasticsearch.plugin.nlpcn;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.nlpcn.es4sql.domain.Select;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by Eliran on 2/9/2016.
 */
public class ElasticUtils {

    public static SearchResponse scrollOneTimeWithHits(Client client, SearchRequestBuilder requestBuilder, Select originalSelect, int resultSize) {
        SearchResponse responseWithHits;SearchRequestBuilder scrollRequest = requestBuilder
                .setScroll(new TimeValue(60000))
                .setSize(resultSize);
        boolean ordered = originalSelect.isOrderdSelect();
        if(!ordered) scrollRequest.setSearchType(SearchType.DEFAULT);
        responseWithHits = scrollRequest.get();
        //on ordered select - not using SCAN , elastic returns hits on first scroll
        if(!ordered) {
            responseWithHits = client.prepareSearchScroll(responseWithHits.getScrollId()).setScroll(new TimeValue(600000)).get();
        }
        return responseWithHits;
    }


    //use our deserializer instead of results toXcontent because the source field is differnet from sourceAsMap.
    public static String hitsAsStringResult(SearchHits results, MetaSearchResult metaResults) throws IOException {
        if(results == null) return null;
        Object[] searchHits;
        searchHits = new Object[(int) results.totalHits()];
        int i = 0;
        for(SearchHit hit : results) {
            HashMap<String,Object> value = new HashMap<>();
            value.put("_id",hit.getId());
            value.put("_type", hit.getType());
            value.put("_score", hit.score());
            value.put("_source", hit.sourceAsMap());
            searchHits[i] = value;
            i++;
        }
        HashMap<String,Object> hits = new HashMap<>();
        hits.put("total",results.totalHits());
        hits.put("max_score",results.maxScore());
        hits.put("hits",searchHits);
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON).prettyPrint();
        builder.startObject();
        builder.field("took", metaResults.getTookImMilli());
        builder.field("timed_out",metaResults.isTimedOut());
        builder.field("_shards", ImmutableMap.of("total", metaResults.getTotalNumOfShards(),
                "successful", metaResults.getSuccessfulShards()
                , "failed", metaResults.getFailedShards()));
        builder.field("hits",hits) ;
        builder.endObject();

        return builder.string();
    }
}
