package org.elasticsearch.plugin.nlpcn;

import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.nlpcn.es4sql.domain.Condition;
import org.nlpcn.es4sql.domain.Select;
import org.nlpcn.es4sql.domain.Where;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.DefaultQueryAction;
import org.nlpcn.es4sql.query.join.NestedLoopsElasticRequestBuilder;
import org.nlpcn.es4sql.query.join.TableInJoinRequestBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Eliran on 15/9/2015.
 */
public class NestedLoopsElasticExecutor extends ElasticJoinExecutor {

    private final NestedLoopsElasticRequestBuilder nestedLoopsRequest;
    private final Client client;

    public NestedLoopsElasticExecutor(Client client, NestedLoopsElasticRequestBuilder nestedLoops) {
        super(nestedLoops);
        this.client = client;
        this.nestedLoopsRequest = nestedLoops;
    }

    @Override
    protected List<SearchHit> innerRun() throws SqlParseException {
        List<SearchHit> combinedResults = new ArrayList<>();
        int totalLimit = nestedLoopsRequest.getTotalLimit();
        int multiSearchMaxSize = nestedLoopsRequest.getMultiSearchMaxSize();
        Select secondTableSelect = nestedLoopsRequest.getSecondTable().getOriginalSelect();
        Where originalSecondTableWhere = secondTableSelect.getWhere();

        orderConditions(nestedLoopsRequest.getFirstTable().getAlias(),nestedLoopsRequest.getSecondTable().getAlias());


        FetchWithScrollResponse fetchWithScrollResponse = firstFetch(this.nestedLoopsRequest.getFirstTable());
        SearchResponse firstTableResponse = fetchWithScrollResponse.getResponse();
        boolean needScrollForFirstTable = fetchWithScrollResponse.isNeedScrollForFirstTable();

        int currentCombinedResults = 0;
        boolean finishedWithFirstTable = false;

        while (totalLimit > currentCombinedResults && !finishedWithFirstTable){

            SearchHit[] hits = firstTableResponse.getHits().getHits();
            boolean finishedMultiSearches = hits.length == 0;
            int currentHitsIndex = 0 ;

            while(!finishedMultiSearches){
                MultiSearchRequest multiSearchRequest = createMultiSearchRequest(multiSearchMaxSize, nestedLoopsRequest.getConnectedWhere(), hits, secondTableSelect, originalSecondTableWhere, currentHitsIndex);
                int multiSearchSize = multiSearchRequest.requests().size();
                currentCombinedResults = combineResultsFromMultiResponses(combinedResults, totalLimit, currentCombinedResults, hits, currentHitsIndex, multiSearchRequest);
                currentHitsIndex += multiSearchSize;
                finishedMultiSearches = currentHitsIndex >= hits.length-1 || currentCombinedResults >= totalLimit;
            }

            if( hits.length < MAX_RESULTS_ON_ONE_FETCH ) needScrollForFirstTable = false;

            if(!finishedWithFirstTable)
            {
                if(needScrollForFirstTable)
                    firstTableResponse = client.prepareSearchScroll(firstTableResponse.getScrollId()).setScroll(new TimeValue(600000)).get();
                else finishedWithFirstTable = true;
            }

        }
        return combinedResults;
    }

    private int combineResultsFromMultiResponses(List<SearchHit> combinedResults, int totalLimit, int currentCombinedResults, SearchHit[] hits, int currentIndex, MultiSearchRequest multiSearchRequest) {
        MultiSearchResponse.Item[] responses = client.multiSearch(multiSearchRequest).actionGet().getResponses();
        String t1Alias = nestedLoopsRequest.getFirstTable().getAlias();
        String t2Alias = nestedLoopsRequest.getSecondTable().getAlias();

        for(int j =0 ; j < responses.length && currentCombinedResults < totalLimit ; j++){
            SearchHit hitFromFirstTable = hits[currentIndex+j];
            onlyReturnedFields(hitFromFirstTable.getSourceAsMap(), nestedLoopsRequest.getFirstTable().getReturnedFields(),nestedLoopsRequest.getFirstTable().getOriginalSelect().isSelectAll());

            SearchResponse multiItemResponse = responses[j].getResponse();
            updateMetaSearchResults(multiItemResponse);

            //todo: if responseForHit.getHits.length < responseForHit.getTotalHits(). need to fetch more!
            SearchHits responseForHit = multiItemResponse.getHits();

            if(responseForHit.getHits().length == 0 && nestedLoopsRequest.getJoinType() == SQLJoinTableSource.JoinType.LEFT_OUTER_JOIN){
                SearchHit unmachedResult = createUnmachedResult(nestedLoopsRequest.getSecondTable().getReturnedFields(), currentCombinedResults, t1Alias, t2Alias, hitFromFirstTable);
                combinedResults.add(unmachedResult);
                currentCombinedResults++;
                continue;
            }

            for(SearchHit matchedHit : responseForHit.getHits() ){
                SearchHit searchHit = getMergedHit(currentCombinedResults, t1Alias, t2Alias, hitFromFirstTable, matchedHit);
                combinedResults.add(searchHit);
                currentCombinedResults++;
                if(currentCombinedResults >= totalLimit) break;
            }
            if(currentCombinedResults >= totalLimit) break;

        }
        return currentCombinedResults;
    }

    private SearchHit getMergedHit(int currentCombinedResults, String t1Alias, String t2Alias, SearchHit hitFromFirstTable, SearchHit matchedHit) {
        onlyReturnedFields(matchedHit.getSourceAsMap(), nestedLoopsRequest.getSecondTable().getReturnedFields(),nestedLoopsRequest.getSecondTable().getOriginalSelect().isSelectAll());
        SearchHit searchHit = new SearchHit(currentCombinedResults, hitFromFirstTable.getId() + "|" + matchedHit.getId(), new Text(hitFromFirstTable.getType() + "|" + matchedHit.getType()), hitFromFirstTable.getFields(), null);
        searchHit.sourceRef(hitFromFirstTable.getSourceRef());
        searchHit.getSourceAsMap().clear();
        searchHit.getSourceAsMap().putAll(hitFromFirstTable.getSourceAsMap());

        mergeSourceAndAddAliases(matchedHit.getSourceAsMap(), searchHit, t1Alias, t2Alias);
        return searchHit;
    }

    private MultiSearchRequest createMultiSearchRequest(int multiSearchMaxSize, Where connectedWhere, SearchHit[] hits, Select secondTableSelect, Where originalWhere, int currentIndex) throws SqlParseException {
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        for(int i = currentIndex  ; i < currentIndex  + multiSearchMaxSize && i< hits.length ; i++ ){
            Map<String, Object> hitFromFirstTableAsMap = hits[i].getSourceAsMap();
            Where newWhere = Where.newInstance();
            if(originalWhere!=null) newWhere.addWhere(originalWhere);
            if(connectedWhere!=null){
                Where connectedWhereCloned = null;
                try {
                    connectedWhereCloned = (Where) connectedWhere.clone();
                } catch (CloneNotSupportedException e) {
                    e.printStackTrace();
                }
                updateValuesOnWhereConditions(hitFromFirstTableAsMap,connectedWhereCloned);
                newWhere.addWhere(connectedWhereCloned);
            }


//            for(Condition c : conditions){
//                Object value = deepSearchInMap(hitFromFirstTableAsMap,c.getValue().toString());
//                Condition conditionWithValue = new Condition(Where.CONN.AND,c.getName(),c.getOpear(),value);
//                newWhere.addWhere(conditionWithValue);
//            }
            //using the 2nd table select and DefaultAction because we can't just change query on request (need to create lot of requests)
            if(newWhere.getWheres().size() != 0) {
                secondTableSelect.setWhere(newWhere);
            }
            DefaultQueryAction action = new DefaultQueryAction(this.client , secondTableSelect);
            action.explain();
            SearchRequestBuilder secondTableRequest = action.getRequestBuilder();
            Integer secondTableHintLimit = this.nestedLoopsRequest.getSecondTable().getHintLimit();
            if(secondTableHintLimit != null && secondTableHintLimit <= MAX_RESULTS_ON_ONE_FETCH)
                secondTableRequest.setSize(secondTableHintLimit);
            multiSearchRequest.add(secondTableRequest);
        }
        return multiSearchRequest;
    }

    private void updateValuesOnWhereConditions(Map<String, Object> hit, Where where) {
        if(where instanceof Condition){
            Condition c = (Condition) where;
            Object value = deepSearchInMap(hit,c.getValue().toString());
            c.setValue(value);
        }
        for(Where innerWhere : where.getWheres()){
            updateValuesOnWhereConditions(hit,innerWhere);
        }
    }

    private FetchWithScrollResponse firstFetch(TableInJoinRequestBuilder tableRequest) {
            Integer hintLimit = tableRequest.getHintLimit();
            boolean needScrollForFirstTable = false;
            SearchResponse responseWithHits;
            if(hintLimit != null && hintLimit < MAX_RESULTS_ON_ONE_FETCH){

                responseWithHits = tableRequest.getRequestBuilder().setSize(hintLimit).get();
                needScrollForFirstTable=false;
            }
            else {
                //scroll request with max.
                responseWithHits = scrollOneTimeWithMax(client,tableRequest);
                if(responseWithHits.getHits().getTotalHits().value < MAX_RESULTS_ON_ONE_FETCH)
                    needScrollForFirstTable = true;
            }

            updateMetaSearchResults(responseWithHits);
            return new FetchWithScrollResponse(responseWithHits,needScrollForFirstTable);
    }



    private void orderConditions(String t1Alias, String t2Alias) {
        orderConditionRecursive(t1Alias,t2Alias,nestedLoopsRequest.getConnectedWhere());
//        Collection<Condition> conditions = nestedLoopsRequest.getT1FieldToCondition().values();
//        for(Condition c : conditions){
//            //TODO: support all orders and for each OPEAR find his related OPEAR (< is > , EQ is EQ ,etc..)
//            if(!c.getName().startsWith(t2Alias+".") || !c.getValue().toString().startsWith(t1Alias +"."))
//                throw new RuntimeException("On NestedLoops currently only supported Ordered conditions (t2.field2 OPEAR t1.field1) , badCondition was:" + c);
//            c.setName(c.getName().replaceFirst(t2Alias+".",""));
//            c.setValue(c.getValue().toString().replaceFirst(t1Alias+ ".", ""));
//        }
    }

    private void orderConditionRecursive(String t1Alias, String t2Alias, Where where) {
        if(where == null) return;
        if(where instanceof Condition){
            Condition c = (Condition) where;
            if(!c.getName().startsWith(t2Alias+".") || !c.getValue().toString().startsWith(t1Alias +"."))
                throw new RuntimeException("On NestedLoops currently only supported Ordered conditions (t2.field2 OPEAR t1.field1) , badCondition was:" + c);
            c.setName(c.getName().replaceFirst(t2Alias+".",""));
            c.setValue(c.getValue().toString().replaceFirst(t1Alias+ ".", ""));
            return;
        }
        else {
            for (Where innerWhere : where.getWheres())
                orderConditionRecursive(t1Alias,t2Alias,innerWhere);
        }
    }


    private class FetchWithScrollResponse {
        private SearchResponse response;
        private boolean needScrollForFirstTable;

        private FetchWithScrollResponse(SearchResponse response, boolean needScrollForFirstTable) {
            this.response = response;
            this.needScrollForFirstTable = needScrollForFirstTable;
        }

        public SearchResponse getResponse() {
            return response;
        }

        public boolean isNeedScrollForFirstTable() {
            return needScrollForFirstTable;
        }

    }
}
