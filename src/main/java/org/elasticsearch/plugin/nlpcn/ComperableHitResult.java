package org.elasticsearch.plugin.nlpcn;

import com.google.common.base.Joiner;
import org.elasticsearch.search.SearchHit;
import org.nlpcn.es4sql.Util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Eliran on 9/9/2016.
 */
public class ComperableHitResult {
    private SearchHit hit;
    private String comperator;
    private boolean isAllNull;
    private Map<String,Object> flattenMap;
    public ComperableHitResult(SearchHit hit , String[] fieldsOrder ,String seperator) {
        this.hit = hit;
        Map<String, Object> hitAsMap = hit.getSourceAsMap();
        this.flattenMap = new HashMap<>();
        List<String> results = new ArrayList<>();
        this.isAllNull = true;

        for(int i = 0 ; i< fieldsOrder.length ;i++){
            String field = fieldsOrder[i];
            Object result = Util.deepSearchInMap(hitAsMap,field);
            if(result == null){
                results.add("");
            }
            else {
                this.isAllNull = false;
                results.add(result.toString());
                this.flattenMap.put(field,result);
            }
        }
        this.comperator = Joiner.on(seperator).join(results);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ComperableHitResult that = (ComperableHitResult) o;

        if (!comperator.equals(that.comperator)) return false;

        return true;
    }

    public boolean isAllNull() {
        return isAllNull;
    }

    @Override
    public int hashCode() {
        return comperator.hashCode();
    }

    public String getComperator() {
        return comperator;
    }

    public Map<String, Object> getFlattenMap() {
        return flattenMap;
    }

    public SearchHit getOriginalHit(){
        return hit;
    }
}
