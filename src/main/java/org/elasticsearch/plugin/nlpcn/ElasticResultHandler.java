package org.elasticsearch.plugin.nlpcn;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author zxh
 * @date 2018/8/06 10:11
 */
public class ElasticResultHandler {
    public static Object getFieldValue(SearchHit hit,String field){
        return deepSearchInMap(hit.getSourceAsMap(),field);
    }

    private static Object deepSearchInMap(Map<String, Object> fieldsMap, String name) {
        if(name.contains(".")){
            String[] path = name.split("\\.");
            Map<String,Object> currentObject = fieldsMap;
            for(int i=0;i<path.length-1 ;i++){
                Object valueFromCurrentMap = currentObject.get(path[i]);
                if(valueFromCurrentMap == null) {
                    return null;
                }
                if(!Map.class.isAssignableFrom(valueFromCurrentMap.getClass())) {
                    return null;
                }
                currentObject = (Map<String, Object>) valueFromCurrentMap;
            }
            return currentObject.get(path[path.length-1]);
        }

        return fieldsMap.get(name);
    }

}
