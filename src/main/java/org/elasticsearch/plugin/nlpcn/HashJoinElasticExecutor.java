package org.elasticsearch.plugin.nlpcn;

import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
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
import java.util.regex.MatchResult;

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
        Map<String,SearchHitsResult> comparisonKeyToSearchHits = new HashMap<>();
        List<Map.Entry<Field, Field>> t1ToT2FieldsComparison = requestBuilder.getT1ToT2FieldsComparison();
        int ids = 1;
        for(SearchHit hit : firstTableHits){
            String key = getComparisonKey(t1ToT2FieldsComparison, hit,true);
            SearchHitsResult currentSearchHitsResult = comparisonKeyToSearchHits.get(key);
            if(currentSearchHitsResult == null) {
                currentSearchHitsResult = new SearchHitsResult(new ArrayList<InternalSearchHit>(),false);
                comparisonKeyToSearchHits.put(key,currentSearchHitsResult);
            }
            //int docid , id
            InternalSearchHit searchHit = new InternalSearchHit(ids, hit.id(), new StringText(hit.getType()), hit.getFields());
            searchHit.sourceRef(hit.getSourceRef());

            onlyReturnedFields(searchHit.sourceAsMap(), firstTableRequest.getReturnedFields());
            ids++;
            currentSearchHitsResult.getSearchHits().add(searchHit);
        }


        ids = 0;
        List<InternalSearchHit> finalResult = new ArrayList<>();
        TableInJoinRequestBuilder secondTableRequest = requestBuilder.getSecondTable();

        SearchHits secondTableHits = secondTableRequest.getRequestBuilder().get().getHits();
        for(SearchHit secondTableHit : secondTableHits){

            String key = getComparisonKey(t1ToT2FieldsComparison,secondTableHit,false);

            SearchHitsResult searchHitsResult = comparisonKeyToSearchHits.get(key);

            if(searchHitsResult!=null && searchHitsResult.getSearchHits().size() > 0){
                searchHitsResult.setMatchedWithOtherTable(true);
                List<InternalSearchHit> searchHits = searchHitsResult.getSearchHits();
                for(InternalSearchHit matchingHit : searchHits){
                    onlyReturnedFields(secondTableHit.sourceAsMap(), secondTableRequest.getReturnedFields());

                    //todo: decide which id to put or type. or maby its ok this way. just need to doc.
                    InternalSearchHit searchHit = new InternalSearchHit(ids, matchingHit.id() + "|" + secondTableHit.getId(), new StringText(matchingHit.getType() + "|" + secondTableHit.getType()), matchingHit.getFields());
                    searchHit.sourceRef(matchingHit.getSourceRef());
                    searchHit.sourceAsMap().clear();
                    searchHit.sourceAsMap().putAll(matchingHit.sourceAsMap());
                    mergeSourceAndAddAliases(secondTableHit.getSource(), searchHit);

                    finalResult.add(searchHit);
                    ids++;
                }
            }
        }

        if(requestBuilder.getJoinType() == SQLJoinTableSource.JoinType.LEFT_OUTER_JOIN){
            addUnmatchedResults(finalResult,comparisonKeyToSearchHits.values(),requestBuilder.getSecondTable().getReturnedFields(),ids);
        }
        InternalSearchHit[] hits = finalResult.toArray(new InternalSearchHit[ids]);
        this.results = new InternalSearchHits(hits,ids,1.0f);

    }

    private void addUnmatchedResults(List<InternalSearchHit> finalResult, Collection<SearchHitsResult> firstTableSearchHits, List<Field> secondTableReturnedFields,int currentNumOfIds) {
        for(SearchHitsResult hitsResult : firstTableSearchHits){
            if(!hitsResult.isMatchedWithOtherTable()){
                for(SearchHit hit: hitsResult.getSearchHits() ) {
                    //todo: decide which id to put or type. or maby its ok this way. just need to doc.
                    InternalSearchHit searchHit = new InternalSearchHit(currentNumOfIds, hit.id() + "|0", new StringText(hit.getType() + "|null"), hit.getFields());
                    searchHit.sourceRef(hit.getSourceRef());
                    searchHit.sourceAsMap().clear();
                    searchHit.sourceAsMap().putAll(hit.sourceAsMap());
                    Map<String,Object> emptySecondTableHitSource = createNullsSource(secondTableReturnedFields);
                    mergeSourceAndAddAliases(emptySecondTableHitSource, searchHit);

                    finalResult.add(searchHit);
                    currentNumOfIds++;
                }
            }
        }
    }

    private Map<String, Object> createNullsSource(List<Field> secondTableReturnedFields) {
        Map<String,Object> nulledSource = new HashMap<>();
        for(Field field : secondTableReturnedFields){
            nulledSource.put(field.getName(),null);
        }
        return nulledSource;
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

    private void mergeSourceAndAddAliases(Map<String,Object> secondTableHitSource, InternalSearchHit searchHit) {
        Map<String,Object> results = mapWithAliases(searchHit.getSource(), requestBuilder.getFirstTable().getAlias());
        results.putAll(mapWithAliases(secondTableHitSource, requestBuilder.getSecondTable().getAlias()));
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
