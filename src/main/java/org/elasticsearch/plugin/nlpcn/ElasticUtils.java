package org.elasticsearch.plugin.nlpcn;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
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
        if(!ordered) scrollRequest.addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC);
        responseWithHits = scrollRequest.get();
        //on ordered select - not using SCAN , elastic returns hits on first scroll
        //es5.0 elastic always return docs on scan
//        if(!ordered) {
//            responseWithHits = client.prepareSearchScroll(responseWithHits.getScrollId()).setScroll(new TimeValue(600000)).get();
//        }
        return responseWithHits;
    }


    //use our deserializer instead of results toXcontent because the source field is differnet from sourceAsMap.
    public static XContentBuilder hitsAsXContentBuilder(SearchHits results, MetaSearchResult metaResults) throws IOException {
        if(results == null) return null;
        Object[] searchHits;
        searchHits = new Object[(int) results.getTotalHits()];
        int i = 0;
        for(SearchHit hit : results) {
            HashMap<String,Object> value = new HashMap<>();
            value.put("_id",hit.getId());
            value.put("_type", hit.getType());
            value.put("_score", hit.getScore());
            value.put("_source", hit.getSourceAsMap());
            searchHits[i] = value;
            i++;
        }
        HashMap<String,Object> hits = new HashMap<>();
        hits.put("total",results.getTotalHits());
        hits.put("max_score",results.getMaxScore());
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
        return builder;
    }
}
