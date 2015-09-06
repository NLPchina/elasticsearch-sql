package org.elasticsearch.plugin.nlpcn;

import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.nlpcn.es4sql.domain.Field;
import org.nlpcn.es4sql.domain.Select;
import org.nlpcn.es4sql.domain.Where;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.HashJoinElasticRequestBuilder;
import org.nlpcn.es4sql.query.TableInJoinRequestBuilder;
import org.nlpcn.es4sql.query.maker.FilterMaker;
import org.nlpcn.es4sql.query.maker.QueryMaker;

import java.io.IOException;
import java.util.*;

/**
 * Created by Eliran on 22/8/2015.
 */
public class HashJoinElasticExecutor {
    private HashJoinElasticRequestBuilder requestBuilder;
    private SearchHits results ;
    private MetaSearchResult metaResults;
    private Client client;
    private boolean useQueryTermsFilterOptimization = false;

    private final int MAX_RESULTS_ON_ONE_FETCH = 10000;
    private final int MAX_RESULTS_FOR_FIRST_TABLE = 100000;

    public HashJoinElasticExecutor(Client client,HashJoinElasticRequestBuilder requestBuilder) {
        this.client = client;
        this.requestBuilder = requestBuilder;
        this.useQueryTermsFilterOptimization = requestBuilder.isUseTermFiltersOptimization();
        metaResults = new MetaSearchResult();
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
    public String resultAsString() throws IOException {
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
            builder.field("_shards",ImmutableMap.of("total",metaResults.getTotalNumOfShards(),
                                                    "successful",metaResults.getSuccessfulShards()
                                                    ,"failed",metaResults.getFailedShards()));
            builder.field("hits",hits) ;
        builder.endObject();

        return builder.string();
    }

    public void run() throws IOException, SqlParseException {
        long timeBefore = System.currentTimeMillis();
        Map<String,List<Object>> optimizationTermsFilterStructure = new HashMap<>();
        List<Map.Entry<Field, Field>> t1ToT2FieldsComparison = requestBuilder.getT1ToT2FieldsComparison();

        updateFirstTableLimitIfNeeded();
        TableInJoinRequestBuilder firstTableRequest = requestBuilder.getFirstTable();
        Map<String, SearchHitsResult> comparisonKeyToSearchHits = createKeyToResultsAndFillOptimizationStructure(optimizationTermsFilterStructure, t1ToT2FieldsComparison, firstTableRequest);

        TableInJoinRequestBuilder secondTableRequest = requestBuilder.getSecondTable();
        if(needToOptimize(optimizationTermsFilterStructure)){
            updateRequestWithTermsFilter(optimizationTermsFilterStructure, secondTableRequest);
        }

        List<InternalSearchHit> combinedResult = createCombinedResults(optimizationTermsFilterStructure, t1ToT2FieldsComparison, comparisonKeyToSearchHits, secondTableRequest);

        int currentNumOfResults = combinedResult.size();
        if(requestBuilder.getJoinType() == SQLJoinTableSource.JoinType.LEFT_OUTER_JOIN && currentNumOfResults < requestBuilder.getTotalLimit()){
            addUnmatchedResults(combinedResult,comparisonKeyToSearchHits.values(),requestBuilder.getSecondTable().getReturnedFields(), currentNumOfResults);
        }
        InternalSearchHit[] hits = combinedResult.toArray(new InternalSearchHit[combinedResult.size()]);
        this.results = new InternalSearchHits(hits,combinedResult.size(),1.0f);
        long joinTimeInMilli = System.currentTimeMillis() - timeBefore;
        this.metaResults.setTookImMilli(joinTimeInMilli);
    }

    private void updateFirstTableLimitIfNeeded() {
        if(requestBuilder.getJoinType() == SQLJoinTableSource.JoinType.LEFT_OUTER_JOIN ){
            Integer firstTableHintLimit = requestBuilder.getFirstTable().getHintLimit();
            int totalLimit = requestBuilder.getTotalLimit();
            if(firstTableHintLimit == null || firstTableHintLimit > totalLimit){
                requestBuilder.getFirstTable().setHintLimit(totalLimit);
            }
        }
    }

    private List<InternalSearchHit> createCombinedResults(Map<String, List<Object>> optimizationTermsFilterStructure, List<Map.Entry<Field, Field>> t1ToT2FieldsComparison, Map<String, SearchHitsResult> comparisonKeyToSearchHits, TableInJoinRequestBuilder secondTableRequest) {
        List<InternalSearchHit> combinedResult = new ArrayList<>();
        int resultIds = 0;
        int totalLimit = this.requestBuilder.getTotalLimit();
        Integer hintLimit = secondTableRequest.getHintLimit();
        SearchResponse searchResponse;
        boolean finishedScrolling;
        if(hintLimit!=null && hintLimit < MAX_RESULTS_ON_ONE_FETCH) {
            searchResponse = secondTableRequest.getRequestBuilder().setSize(hintLimit).get();
            finishedScrolling = true;
        }
        else {
            searchResponse = secondTableRequest.getRequestBuilder()
                                            .setSearchType(SearchType.SCAN)
                                            .setScroll(new TimeValue(60000))
                                            .setSize(MAX_RESULTS_ON_ONE_FETCH).get();
            searchResponse = client.prepareSearchScroll(searchResponse.getScrollId()).setScroll(new TimeValue(600000)).get();
            finishedScrolling = false;
        }
        updateMetaSearchResults(searchResponse);

        boolean limitReached =false;
        int fetchedSoFarFromSecondTable = 0;
        while(!limitReached ) {
            SearchHit[] secondTableHits = searchResponse.getHits().getHits();
            fetchedSoFarFromSecondTable += secondTableHits.length;
            for (SearchHit secondTableHit : secondTableHits) {
                if (limitReached) break;
                String key = getComparisonKey(t1ToT2FieldsComparison, secondTableHit, false, optimizationTermsFilterStructure);

                SearchHitsResult searchHitsResult = comparisonKeyToSearchHits.get(key);

                if (searchHitsResult != null && searchHitsResult.getSearchHits().size() > 0) {
                    searchHitsResult.setMatchedWithOtherTable(true);
                    List<InternalSearchHit> searchHits = searchHitsResult.getSearchHits();
                    for (InternalSearchHit matchingHit : searchHits) {
                        onlyReturnedFields(secondTableHit.sourceAsMap(), secondTableRequest.getReturnedFields());

                        //todo: decide which id to put or type. or maby its ok this way. just need to doc.
                        InternalSearchHit searchHit = new InternalSearchHit(resultIds, matchingHit.id() + "|" + secondTableHit.getId(), new StringText(matchingHit.getType() + "|" + secondTableHit.getType()), matchingHit.getFields());
                        searchHit.sourceRef(matchingHit.getSourceRef());
                        searchHit.sourceAsMap().clear();
                        searchHit.sourceAsMap().putAll(matchingHit.sourceAsMap());
                        mergeSourceAndAddAliases(secondTableHit.getSource(), searchHit);

                        combinedResult.add(searchHit);
                        resultIds++;
                        if (resultIds >= totalLimit) {
                            limitReached = true;
                            break;
                        }
                    }
                }
            }
            if(!finishedScrolling){
                if(secondTableHits.length>0 && (hintLimit == null || fetchedSoFarFromSecondTable  >= hintLimit)){
                    searchResponse = client.prepareSearchScroll(searchResponse.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
                }
                else break;
            }
            else {
                break;
            }
        }
        return combinedResult;
    }

    private Map<String, SearchHitsResult> createKeyToResultsAndFillOptimizationStructure(Map<String, List<Object>> optimizationTermsFilterStructure, List<Map.Entry<Field, Field>> t1ToT2FieldsComparison, TableInJoinRequestBuilder firstTableRequest) {
        List<SearchHit> firstTableHits =  fetchAllHits(firstTableRequest.getRequestBuilder(),firstTableRequest.getHintLimit());
        Map<String,SearchHitsResult> comparisonKeyToSearchHits = new HashMap<>();

        int resultIds = 1;
        for(SearchHit hit : firstTableHits){
            String key = getComparisonKey(t1ToT2FieldsComparison, hit,true,optimizationTermsFilterStructure);
            SearchHitsResult currentSearchHitsResult = comparisonKeyToSearchHits.get(key);
            if(currentSearchHitsResult == null) {
                currentSearchHitsResult = new SearchHitsResult(new ArrayList<InternalSearchHit>(),false);
                comparisonKeyToSearchHits.put(key,currentSearchHitsResult);
            }
            //int docid , id
            InternalSearchHit searchHit = new InternalSearchHit(resultIds, hit.id(), new StringText(hit.getType()), hit.getFields());
            searchHit.sourceRef(hit.getSourceRef());

            onlyReturnedFields(searchHit.sourceAsMap(), firstTableRequest.getReturnedFields());
            resultIds++;
            currentSearchHitsResult.getSearchHits().add(searchHit);
        }
        return comparisonKeyToSearchHits;
    }

    private List<SearchHit> fetchAllHits(SearchRequestBuilder requestBuilder, Integer hintLimit) {
        if(hintLimit != null && hintLimit < MAX_RESULTS_ON_ONE_FETCH ) {
            requestBuilder.setSize(hintLimit);
            SearchResponse searchResponse = requestBuilder.get();
            updateMetaSearchResults(searchResponse);
            return  Arrays.asList(searchResponse.getHits().getHits());
        }
        return scrollTillLimit(requestBuilder, hintLimit);
    }

    private List<SearchHit> scrollTillLimit(SearchRequestBuilder requestBuilder, Integer hintLimit) {
        SearchResponse scrollResp = requestBuilder.setSearchType(SearchType.SCAN)
                                                .setScroll(new TimeValue(60000))
                                                .setSize(MAX_RESULTS_ON_ONE_FETCH).get();

        scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).get();
        updateMetaSearchResults(scrollResp);
        List<SearchHit> hitsWithScan = new ArrayList<>();
        int curentNumOfResults = 0;
        SearchHit[] hits = scrollResp.getHits().hits();

        if(hintLimit == null) hintLimit = MAX_RESULTS_FOR_FIRST_TABLE;

        while (hits.length != 0 && curentNumOfResults < hintLimit) {
            curentNumOfResults += hits.length;
            Collections.addAll(hitsWithScan, hits);
            if(curentNumOfResults >= MAX_RESULTS_FOR_FIRST_TABLE ) {
                //todo: log or exception?
                System.out.println("too many results for first table, stoping at:" + curentNumOfResults);
                break;
            }
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
            hits = scrollResp.getHits().getHits();
        }
        return hitsWithScan;
    }

    private void updateMetaSearchResults( SearchResponse searchResponse) {
        this.metaResults.addSuccessfulShards(searchResponse.getSuccessfulShards());
        this.metaResults.addFailedShards(searchResponse.getFailedShards());
        this.metaResults.addTotalNumOfShards(searchResponse.getTotalShards());
        this.metaResults.updateTimeOut(searchResponse.isTimedOut());
    }

    private boolean needToOptimize(Map<String, List<Object>> optimizationTermsFilterStructure) {
        return useQueryTermsFilterOptimization && optimizationTermsFilterStructure!=null && optimizationTermsFilterStructure.size()>0;
    }

    private void updateRequestWithTermsFilter(Map<String, List<Object>> optimizationTermsFilterStructure, TableInJoinRequestBuilder secondTableRequest) throws SqlParseException {
        Select select = secondTableRequest.getOriginalSelect();
        //todo: change to list with or when more than one object is allowed, and do foreach on map
        BoolFilterBuilder andFilter = FilterBuilders.boolFilter();
        BoolQueryBuilder andQuery = QueryBuilders.boolQuery();
        for(Map.Entry<String,List<Object>> keyToValues : optimizationTermsFilterStructure.entrySet()){
            String fieldName = keyToValues.getKey();
            List<Object> values = keyToValues.getValue();
            if(select.isQuery) andQuery.must(QueryBuilders.termsQuery(fieldName, values));
            else andFilter.must(FilterBuilders.termsFilter(fieldName, values));
        }
        Where where = select.getWhere();
        if (select.isQuery) {
            BoolQueryBuilder boolQuery;
            if(where != null ){
                boolQuery = QueryMaker.explan(where);
                boolQuery.must(andQuery);
            }
            else boolQuery = andQuery;
            secondTableRequest.getRequestBuilder().setQuery(boolQuery);
        } else {
            BoolFilterBuilder boolFilter;
            if(where!=null) {
                boolFilter = FilterMaker.explan(where);
                boolFilter.must(andFilter);
            }
            else boolFilter = andFilter;
            secondTableRequest.getRequestBuilder().setQuery(QueryBuilders.filteredQuery(null, boolFilter));
        }
    }

    private void addUnmatchedResults(List<InternalSearchHit> combinedResults, Collection<SearchHitsResult> firstTableSearchHits, List<Field> secondTableReturnedFields,int currentNumOfIds) {
        boolean limitReached = false;
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

                    combinedResults.add(searchHit);
                    currentNumOfIds++;
                    if(currentNumOfIds >= requestBuilder.getTotalLimit()){
                        limitReached = true;
                        break;
                    }

                }
            }
            if(limitReached) break;
        }
    }

    private Map<String, Object> createNullsSource(List<Field> secondTableReturnedFields) {
        Map<String,Object> nulledSource = new HashMap<>();
        for(Field field : secondTableReturnedFields){
            nulledSource.put(field.getName(),null);
        }
        return nulledSource;
    }

    private String getComparisonKey(List<Map.Entry<Field, Field>> t1ToT2FieldsComparison, SearchHit hit, boolean firstTable, Map<String, List<Object>> optimizationTermsFilterStructure) {
        String key = "";
        Map<String, Object> sourceAsMap = hit.sourceAsMap();
        for(Map.Entry<Field,Field> t1ToT2 : t1ToT2FieldsComparison){
            //todo: change to our function find if key contains '.'
            String name;
            if(firstTable) name = t1ToT2.getKey().getName();
            else name = t1ToT2.getValue().getName();

            Object data = deepSearchInMap(sourceAsMap, name);
            if(firstTable && useQueryTermsFilterOptimization){
                updateOptimizationData(optimizationTermsFilterStructure, data, t1ToT2.getValue().getName());
            }
            if(data == null)
                key+="|null|";
            else
                key+="|"+data.toString()+"|";
        }
        return key;
    }

    private void updateOptimizationData(Map<String, List<Object>> optimizationTermsFilterStructure, Object data, String queryOptimizationKey) {
        List<Object> values = optimizationTermsFilterStructure.get(queryOptimizationKey);
        if(values == null){
            values = new ArrayList<>();
            optimizationTermsFilterStructure.put(queryOptimizationKey,values);
        }
        if(data instanceof String){
            //todo: analyzed or not analyzed check..
            data = ((String) data).toLowerCase();
        }
        values.add(data);
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
