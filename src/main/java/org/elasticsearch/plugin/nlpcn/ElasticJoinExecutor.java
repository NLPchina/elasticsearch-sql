package org.elasticsearch.plugin.nlpcn;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.nlpcn.es4sql.domain.Field;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.SqlElasticRequestBuilder;
import org.nlpcn.es4sql.query.join.HashJoinElasticRequestBuilder;
import org.nlpcn.es4sql.query.join.JoinRequestBuilder;
import org.nlpcn.es4sql.query.join.NestedLoopsElasticRequestBuilder;
import org.nlpcn.es4sql.query.join.TableInJoinRequestBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by Eliran on 15/9/2015.
 */
public abstract class ElasticJoinExecutor implements ElasticHitsExecutor {
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

    public void sendResponse(RestChannel channel) throws IOException {
        XContentBuilder builder = ElasticUtils.hitsAsXContentBuilder(results, metaResults);
        RestResponse bytesRestResponse = new RestResponse(RestStatus.OK, builder);
        channel.sendResponse(bytesRestResponse);
    }

    @Override
    public void run() throws IOException, SqlParseException {
        long timeBefore = System.currentTimeMillis();
        List<SearchHit> combinedSearchHits =  innerRun();
        int resultsSize = combinedSearchHits.size();
        SearchHit[] hits = combinedSearchHits.toArray(new SearchHit[resultsSize]);
        this.results = SearchHits.unpooled(hits, new TotalHits(resultsSize, TotalHits.Relation.EQUAL_TO), 1.0f);
        long joinTimeInMilli = System.currentTimeMillis() - timeBefore;
        this.metaResults.setTookImMilli(joinTimeInMilli);
    }


    protected abstract List<SearchHit> innerRun() throws IOException, SqlParseException ;

    @Override
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

    protected void mergeSourceAndAddAliases(Map<String,Object> secondTableHitSource, Map<String, Object> hitSource, String t1Alias, String t2Alias) {
        Map<String,Object> results = mapWithAliases(hitSource, t1Alias);
        results.putAll(mapWithAliases(secondTableHitSource, t2Alias));
        hitSource.clear();
        hitSource.putAll(results);
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

    protected void  onlyReturnedFields(Map<String, Object> fieldsMap, List<Field> required,boolean allRequired) {
        HashMap<String,Object> filteredMap = new HashMap<>();
        if(allFieldsReturn || allRequired) {
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


    protected void addUnmatchedResults(List<SearchHit> combinedResults, Collection<SearchHitsResult> firstTableSearchHits, List<Field> secondTableReturnedFields,int currentNumOfIds, int totalLimit,String t1Alias,String t2Alias) {
        boolean limitReached = false;
        for(SearchHitsResult hitsResult : firstTableSearchHits){
            if(!hitsResult.isMatchedWithOtherTable())
                for (SearchHit hit : hitsResult.getSearchHits()) {

                    //todo: decide which id to put or type. or maby its ok this way. just need to doc.
                    SearchHit unmachedResult = createUnmachedResult(secondTableReturnedFields, hit.docId(), t1Alias, t2Alias, hit);
                    combinedResults.add(unmachedResult);
                    currentNumOfIds++;
                    if (currentNumOfIds >= totalLimit) {
                        limitReached = true;
                        break;
                    }

                }
            if(limitReached) break;
        }
    }

    protected SearchHit createUnmachedResult( List<Field> secondTableReturnedFields, int docId, String t1Alias, String t2Alias, SearchHit hit) {
        String unmatchedId = hit.getId() + "|0";

        SearchHit searchHit = SearchHit.unpooled(docId, unmatchedId);
        searchHit.addDocumentFields(hit.getDocumentFields(), Collections.emptyMap());
        searchHit.sourceRef(hit.getSourceRef());
        Source source = Source.fromBytes(searchHit.getSourceRef());
        Map<String, Object> hitSource = source.source();
        hitSource.clear();
        hitSource.putAll(Source.fromBytes(hit.getSourceRef()).source());
        Map<String,Object> emptySecondTableHitSource = createNullsSource(secondTableReturnedFields);

        mergeSourceAndAddAliases(emptySecondTableHitSource, hitSource, t1Alias,t2Alias);
        searchHit.sourceRef(Source.fromMap(hitSource, source.sourceContentType()).internalSourceRef());

        return searchHit;
    }

    protected Map<String, Object> createNullsSource(List<Field> secondTableReturnedFields) {
        Map<String,Object> nulledSource = new HashMap<>();
        for(Field field : secondTableReturnedFields){
            if(!field.getName().equals("*")){
                nulledSource.put(field.getName(),null);
            }
        }
        return nulledSource;
    }

    protected void updateMetaSearchResults( SearchResponse searchResponse) {
        this.metaResults.addSuccessfulShards(searchResponse.getSuccessfulShards());
        this.metaResults.addFailedShards(searchResponse.getFailedShards());
        this.metaResults.addTotalNumOfShards(searchResponse.getTotalShards());
        this.metaResults.updateTimeOut(searchResponse.isTimedOut());
    }

    protected SearchResponse scrollOneTimeWithMax(Client client,TableInJoinRequestBuilder tableRequest) {
        SearchResponse responseWithHits;SearchRequestBuilder scrollRequest = tableRequest.getRequestBuilder()
                .setScroll(new TimeValue(60000))
                .setSize(MAX_RESULTS_ON_ONE_FETCH);
        boolean ordered = tableRequest.getOriginalSelect().isOrderdSelect();
        if(!ordered) scrollRequest.addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC);
        responseWithHits = scrollRequest.get();
        //on ordered select - not using SCAN , elastic returns hits on first scroll
        //es5.0 elastic always return docs on scan
//        if(!ordered)
//            responseWithHits = client.prepareSearchScroll(responseWithHits.getScrollId()).setScroll(new TimeValue(600000)).get();
        return responseWithHits;
    }


}
