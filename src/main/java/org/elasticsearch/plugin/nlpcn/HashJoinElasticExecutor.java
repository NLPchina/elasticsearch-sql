package org.elasticsearch.plugin.nlpcn;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.nlpcn.es4sql.domain.Field;
import org.nlpcn.es4sql.query.HashJoinElasticRequestBuilder;
import org.nlpcn.es4sql.query.TableInJoinRequestBuilder;

import java.io.IOException;
import java.util.*;

/**
 * Created by Eliran on 22/8/2015.
 */
public class HashJoinElasticExecutor {
    private HashJoinElasticRequestBuilder requestBuilder;
    private SearchHits results ;
    private Client client;

    public HashJoinElasticExecutor(Client client,HashJoinElasticRequestBuilder requestBuilder) {
        this.client = client;
        this.requestBuilder = requestBuilder;
    }

    public SearchHits getHits(){
        return results;
    }
    public void sendResponse(RestChannel channel){
        try {
            String json = resultAsString();
            BytesRestResponse bytesRestResponse = new BytesRestResponse(RestStatus.OK, json);
            channel.sendResponse(bytesRestResponse);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    //use our deserializer instead of results toXcontent because the source field is differnet from sourceAsMap.
    private String resultAsString() throws IOException {
        Object[] searchHits;
        searchHits = new Object[(int) this.results.totalHits()];
        int i = 0;
        for(SearchHit hit : this.results) {
            HashMap<String,Object> value = new HashMap<>();
            value.put("_id",hit.getId());
            value.put("_type", hit.getType());
            value.put("_score", hit.score());
            value.put("_source", hit.sourceAsMap());
            searchHits[i] = value;
            i++;
        }
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON).prettyPrint();

        builder.startObject("hits");
            builder.field("total").value(this.results.totalHits());
            builder.field("max_score").value(this.results.maxScore());
            builder.array("hits",searchHits);
        builder.endObject();
        return builder.string();
    }

    public void run() throws IOException {
        TableInJoinRequestBuilder firstTableRequest = requestBuilder.getFirstTable();
        SearchHits firstTableHits = firstTableRequest.getRequestBuilder().get().getHits();
        Map<String,List<InternalSearchHit>> comparisonKeyToSearchHits = new HashMap<>();
        List<Map.Entry<Field, Field>> t1ToT2FieldsComparison = requestBuilder.getT1ToT2FieldsComparison();
        int ids = 1;
        for(SearchHit hit : firstTableHits){
            String key = getComparisonKey(t1ToT2FieldsComparison, hit,true);
            List<InternalSearchHit> currentSearchHits = comparisonKeyToSearchHits.get(key);
            if(currentSearchHits == null) {
                currentSearchHits = new ArrayList<>();
                comparisonKeyToSearchHits.put(key,currentSearchHits);
            }
            //int docid , id
            InternalSearchHit searchHit = new InternalSearchHit(ids, hit.id(), new StringText(hit.getType()), hit.getFields());
            searchHit.sourceRef(hit.getSourceRef());

            onlyReturnedFields(searchHit.sourceAsMap(), firstTableRequest.getReturnedFields());
            ids++;
            currentSearchHits.add(searchHit);
        }


        ids = 0;
        List<InternalSearchHit> finalResult = new ArrayList<>();
        TableInJoinRequestBuilder secondTableRequest = requestBuilder.getSecondTable();

        SearchHits secondTableHits = secondTableRequest.getRequestBuilder().get().getHits();
        for(SearchHit secondTableHit : secondTableHits){

            String key = getComparisonKey(t1ToT2FieldsComparison,secondTableHit,false);

            List<InternalSearchHit> searchHits = comparisonKeyToSearchHits.get(key);
            //TODO decide what to do according to left join. now assume regular join.
            if(searchHits!=null && searchHits.size() > 0){
                for(InternalSearchHit matchingHit : searchHits){
                    onlyReturnedFields(secondTableHit.sourceAsMap(), secondTableRequest.getReturnedFields());

                    //todo: decide which id to put or type. or maby its ok this way. just need to doc.
                    InternalSearchHit searchHit = new InternalSearchHit(ids, matchingHit.id() + "|" + secondTableHit.getId(), new StringText(matchingHit.getType() + "|" + secondTableHit.getType()), matchingHit.getFields());
                    searchHit.sourceRef(matchingHit.getSourceRef());
                    searchHit.sourceAsMap().clear();
                    searchHit.sourceAsMap().putAll(matchingHit.sourceAsMap());
                    mergeSourceAndAddAliases(secondTableHit, searchHit);

                    finalResult.add(searchHit);
                    ids++;
                }
            }
        }

        InternalSearchHit[] hits = finalResult.toArray(new InternalSearchHit[ids]);
        this.results = new InternalSearchHits(hits,ids,1.0f);

    }

    private String getComparisonKey(List<Map.Entry<Field, Field>> t1ToT2FieldsComparison, SearchHit hit, boolean firstTable) {
        String key = "";
        Map<String, Object> sourceAsMap = hit.sourceAsMap();
        for(Map.Entry<Field,Field> t1ToT2 : t1ToT2FieldsComparison){
            //todo: change to our function find if key contains '.'
            String name;
            if(firstTable) name = t1ToT2.getKey().getName();
            else name = t1ToT2.getValue().getName();

            Object data = deepSearchInMap(sourceAsMap, name);
            if(data == null)
                key+="|null|";
            else
                key+="|"+data.toString()+"|";
        }
        return key;
    }

    private void mergeSourceAndAddAliases(SearchHit secondTableHit, InternalSearchHit searchHit) {
        Map<String,Object> results = mapWithAliases(searchHit.getSource(), requestBuilder.getFirstTable().getAlias());
        results.putAll(mapWithAliases(secondTableHit.getSource(), requestBuilder.getSecondTable().getAlias()));
        searchHit.getSource().clear();
        searchHit.getSource().putAll(results);
    }

    private  Map<String,Object> mapWithAliases(Map<String, Object> source, String alias) {
        Map<String,Object> mapWithAliases = new HashMap<>();
        for(Map.Entry<String,Object> fieldNameToValue : source.entrySet()) {
            mapWithAliases.put(alias + "." + fieldNameToValue.getKey(), fieldNameToValue.getValue());
        }
        return mapWithAliases;
    }

    private void  onlyReturnedFields(Map<String, Object> fieldsMap, List<Field> required) {
        HashMap<String,Object> filteredMap = new HashMap<>();

        for(Field field: required){
            String name = field.getName();
            filteredMap.put(name, deepSearchInMap(fieldsMap, name));
        }
        fieldsMap.clear();
        fieldsMap.putAll(filteredMap);

    }

    private Object deepSearchInMap(Map<String, Object> fieldsMap, String name) {
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
