package org.elasticsearch.plugin.nlpcn;

import java.util.Map;

import org.elasticsearch.search.SearchHit;

/**
 * Created by Eliran on 3/10/2015.
 */
public class ElasticResultHandler {
    public static Object getFieldValue(SearchHit hit,String field){
        return deepSearchInMap(hit.sourceAsMap(),field);
    }

    private static Object deepSearchInMap(Map<String, Object> fieldsMap, String name) {
        if(name.contains(".")){
            String[] path = name.split("\\.");
            Map<String,Object> currentObject = fieldsMap;
            for(int i=0;i<path.length-1 ;i++){
                Object valueFromCurrentMap = currentObject.get(path[i]);
                if(valueFromCurrentMap == null) return null;
                if(!Map.class.isAssignableFrom(valueFromCurrentMap.getClass())) return null;
                currentObject = (Map<String, Object>) valueFromCurrentMap;
            }
            return currentObject.get(path[path.length-1]);
        }

        return fieldsMap.get(name);
    }

}
