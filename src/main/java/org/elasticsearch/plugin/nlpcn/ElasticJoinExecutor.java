package org.elasticsearch.plugin.nlpcn;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.search.SearchResponse;
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
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.SqlElasticRequestBuilder;
import org.nlpcn.es4sql.query.join.HashJoinElasticRequestBuilder;
import org.nlpcn.es4sql.query.join.JoinRequestBuilder;
import org.nlpcn.es4sql.query.join.NestedLoopsElasticRequestBuilder;

import java.io.IOException;
import java.util.*;

/**
 * Created by Eliran on 15/9/2015.
 */
public abstract class ElasticJoinExecutor {
    protected SearchHits results ;
    protected MetaSearchResult metaResults;
    protected final int MAX_RESULTS_ON_ONE_FETCH = 10000;
    private Set<String> aliasesOnReturn;
    private boolean allFieldsReturn;

    protected ElasticJoinExecutor(JoinRequestBuilder requestBuilder) {
        metaResults = new MetaSearchResult();
        aliasesOnReturn = new HashSet<>();
        List<Field> firstTableReturnedField = requestBuilder.getFirstTable().getReturnedFields();
        List<Field> secondTableReturnedField = requestBuilder.getSecondTable().getReturnedFields();
        allFieldsReturn = (firstTableReturnedField == null || firstTableReturnedField.size() == 0)
                            && (secondTableReturnedField == null || secondTableReturnedField.size() == 0);
    }

    public void  sendResponse(RestChannel channel){
        try {
            String json = resultAsString();
            BytesRestResponse bytesRestResponse = new BytesRestResponse(RestStatus.OK, json);
            channel.sendResponse(bytesRestResponse);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() throws IOException, SqlParseException {
        long timeBefore = System.currentTimeMillis();
        List<InternalSearchHit> combinedSearchHits =  innerRun();
        int resultsSize = combinedSearchHits.size();
        InternalSearchHit[] hits = combinedSearchHits.toArray(new InternalSearchHit[resultsSize]);
        this.results = new InternalSearchHits(hits, resultsSize,1.0f);
        long joinTimeInMilli = System.currentTimeMillis() - timeBefore;
        this.metaResults.setTookImMilli(joinTimeInMilli);
    }

    //use our deserializer instead of results toXcontent because the source field is differnet from sourceAsMap.
    public String resultAsString() throws IOException {
        if(this.results == null) return null;
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
        HashMap<String,Object> hits = new HashMap<>();
        hits.put("total",this.results.totalHits());
        hits.put("max_score",this.results.maxScore());
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

    protected abstract List<InternalSearchHit> innerRun() throws IOException, SqlParseException ;

    public SearchHits getHits(){
        return results;
    }

    public static ElasticJoinExecutor createJoinExecutor(Client client, SqlElasticRequestBuilder requestBuilder){
        if(requestBuilder instanceof HashJoinElasticRequestBuilder) {
            HashJoinElasticRequestBuilder hashJoin = (HashJoinElasticRequestBuilder) requestBuilder;
            return new HashJoinElasticExecutor(client, hashJoin);
        }
        else if (requestBuilder instanceof NestedLoopsElasticRequestBuilder){
            NestedLoopsElasticRequestBuilder nestedLoops = (NestedLoopsElasticRequestBuilder) requestBuilder;
            return  new NestedLoopsElasticExecutor(client,nestedLoops);
        }
        else {
            throw new RuntimeException("Unsuported requestBuilder of type: " + requestBuilder.getClass());
        }
    }

    protected void mergeSourceAndAddAliases(Map<String,Object> secondTableHitSource, InternalSearchHit searchHit,String t1Alias,String t2Alias) {
        Map<String,Object> results = mapWithAliases(searchHit.getSource(), t1Alias);
        results.putAll(mapWithAliases(secondTableHitSource, t2Alias));
        searchHit.getSource().clear();
        searchHit.getSource().putAll(results);
    }

    protected Map<String,Object> mapWithAliases(Map<String, Object> source, String alias) {
        Map<String,Object> mapWithAliases = new HashMap<>();
        for(Map.Entry<String,Object> fieldNameToValue : source.entrySet()) {
            if(!aliasesOnReturn.contains(fieldNameToValue.getKey()))
                mapWithAliases.put(alias + "." + fieldNameToValue.getKey(), fieldNameToValue.getValue());
            else mapWithAliases.put(fieldNameToValue.getKey(),fieldNameToValue.getValue());
        }
        return mapWithAliases;
    }

    protected void  onlyReturnedFields(Map<String, Object> fieldsMap, List<Field> required) {
        HashMap<String,Object> filteredMap = new HashMap<>();
        if(allFieldsReturn) {
            filteredMap.putAll(fieldsMap);
            return;
        }
        for(Field field: required){
            String name = field.getName();
            String returnName = name;
            String alias = field.getAlias();
            if(alias !=null && alias !=""){
                returnName = alias;
                aliasesOnReturn.add(alias);
            }
            filteredMap.put(returnName, deepSearchInMap(fieldsMap, name));
        }
        fieldsMap.clear();
        fieldsMap.putAll(filteredMap);

    }

    protected Object deepSearchInMap(Map<String, Object> fieldsMap, String name) {
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


    protected void addUnmatchedResults(List<InternalSearchHit> combinedResults, Collection<SearchHitsResult> firstTableSearchHits, List<Field> secondTableReturnedFields,int currentNumOfIds, int totalLimit,String t1Alias,String t2Alias) {
        boolean limitReached = false;
        for(SearchHitsResult hitsResult : firstTableSearchHits){
            if(!hitsResult.isMatchedWithOtherTable()){
                for(SearchHit hit: hitsResult.getSearchHits() ) {

                    //todo: decide which id to put or type. or maby its ok this way. just need to doc.
                    addUnmachedResult(combinedResults, secondTableReturnedFields, currentNumOfIds, t1Alias, t2Alias, hit);
                    currentNumOfIds++;
                    if(currentNumOfIds >= totalLimit){
                        limitReached = true;
                        break;
                    }

                }
            }
            if(limitReached) break;
        }
    }

    protected void addUnmachedResult(List<InternalSearchHit> combinedResults, List<Field> secondTableReturnedFields, int currentNumOfIds, String t1Alias, String t2Alias, SearchHit hit) {
        InternalSearchHit searchHit = new InternalSearchHit(currentNumOfIds, hit.id() + "|0", new StringText(hit.getType() + "|null"), hit.getFields());
        searchHit.sourceRef(hit.getSourceRef());
        searchHit.sourceAsMap().clear();
        searchHit.sourceAsMap().putAll(hit.sourceAsMap());
        Map<String,Object> emptySecondTableHitSource = createNullsSource(secondTableReturnedFields);

        mergeSourceAndAddAliases(emptySecondTableHitSource, searchHit,t1Alias,t2Alias);

        combinedResults.add(searchHit);
    }

    protected Map<String, Object> createNullsSource(List<Field> secondTableReturnedFields) {
        Map<String,Object> nulledSource = new HashMap<>();
        for(Field field : secondTableReturnedFields){
            nulledSource.put(field.getName(),null);
        }
        return nulledSource;
    }

    protected void updateMetaSearchResults( SearchResponse searchResponse) {
        this.metaResults.addSuccessfulShards(searchResponse.getSuccessfulShards());
        this.metaResults.addFailedShards(searchResponse.getFailedShards());
        this.metaResults.addTotalNumOfShards(searchResponse.getTotalShards());
        this.metaResults.updateTimeOut(searchResponse.isTimedOut());
    }


}
