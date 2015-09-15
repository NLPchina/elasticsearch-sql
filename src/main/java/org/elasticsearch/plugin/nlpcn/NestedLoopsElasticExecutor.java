package org.elasticsearch.plugin.nlpcn;

import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.nlpcn.es4sql.domain.Condition;
import org.nlpcn.es4sql.domain.Field;
import org.nlpcn.es4sql.domain.Select;
import org.nlpcn.es4sql.domain.Where;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.DefaultQueryAction;
import org.nlpcn.es4sql.query.join.NestedLoopsElasticRequestBuilder;
import org.nlpcn.es4sql.query.maker.FilterMaker;
import org.nlpcn.es4sql.query.maker.QueryMaker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by Eliran on 15/9/2015.
 */
public class NestedLoopsElasticExecutor extends ElasticJoinExecutor {
    private static final int MULTI_SIZE = 100;
    private final NestedLoopsElasticRequestBuilder

            nestedLoopsRequest;
    private final Client client;

    public NestedLoopsElasticExecutor(Client client, NestedLoopsElasticRequestBuilder nestedLoops) {
        this.client = client;
        this.nestedLoopsRequest = nestedLoops;
    }

    @Override
    protected List<InternalSearchHit> innerRun() throws SqlParseException {
        List<InternalSearchHit> combinedResults = new ArrayList<>();
        int totalLimit = nestedLoopsRequest.getTotalLimit();
        SearchResponse response = nestedLoopsRequest.getFirstTable().getRequestBuilder().get();
        orderConditions(nestedLoopsRequest.getFirstTable().getAlias(),nestedLoopsRequest.getSecondTable().getAlias());
        Collection<Condition> conditions = nestedLoopsRequest.getT1FieldToCondition().values();
        int currentCombinedResults = 0;
        boolean finishedWithFirstTable = false;
        while (totalLimit > currentCombinedResults && !finishedWithFirstTable){
            SearchHit[] hits = response.getHits().getHits();
            if(hits.length  == 0 || hits.length < MAX_RESULTS_ON_ONE_FETCH) finishedWithFirstTable = true;
            Select secondTableSelect = nestedLoopsRequest.getSecondTable().getOriginalSelect();
            Where originalWhere = secondTableSelect.getWhere();
            String t1Alias = nestedLoopsRequest.getFirstTable().getAlias();
            String t2Alias = nestedLoopsRequest.getSecondTable().getAlias();
            boolean finishedMultiSearches = hits.length == 0;
            int currentIndex = 0 ;

            while(!finishedMultiSearches){
                MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
                int multiSearchSize = 0;
                for(int i = currentIndex  ; i < currentIndex  + MULTI_SIZE && i< hits.length ; i++ ){
                    Map<String, Object> hitFromFirstTableAsMap = hits[i].sourceAsMap();
                    Where newWhere = Where.newInstance();
                    if(originalWhere!=null) newWhere.addWhere(originalWhere);
                    for(Condition c : conditions){
                        Object value = deepSearchInMap(hitFromFirstTableAsMap,c.getValue().toString());
                        Condition conditionWithValue = new Condition(Where.CONN.AND,c.getName(),c.getOpear(),value);
                        newWhere.addWhere(conditionWithValue);
                    }
                    //using the 2nd table select and DefaultAction because we can't just change query on request (need to create lot of requests)
                    if(newWhere.getWheres().size() != 0) {
                        secondTableSelect.setWhere(newWhere);
                    }
                    DefaultQueryAction action = new DefaultQueryAction(this.client , secondTableSelect);
                    action.explain();
                    multiSearchRequest.add(action.getRequestBuilder());
                    multiSearchSize++;
                }
                MultiSearchResponse.Item[] responses = client.multiSearch(multiSearchRequest).actionGet().getResponses();
                for(int j =0 ; j < responses.length && currentCombinedResults < totalLimit ; j++){
                    SearchHits responseForHit = responses[j].getResponse().getHits();
                    SearchHit hitFromFirstTable = hits[currentIndex+j];
                    //todo: if responseForHit.getHits.length < responseForHit.getTotalHits(). need to fetch more!
                    onlyReturnedFields(hitFromFirstTable.sourceAsMap(), nestedLoopsRequest.getFirstTable().getReturnedFields());
                    if(responseForHit.getHits().length == 0 && nestedLoopsRequest.getJoinType() == SQLJoinTableSource.JoinType.LEFT_OUTER_JOIN){
                        addUnmachedResult(combinedResults, nestedLoopsRequest.getSecondTable().getReturnedFields(), currentCombinedResults, t1Alias, t2Alias, hitFromFirstTable);
                        currentCombinedResults++;

                        continue;
                    }

                    for(SearchHit matchedHit : responseForHit.getHits() ){
                        onlyReturnedFields(matchedHit.sourceAsMap(), nestedLoopsRequest.getSecondTable().getReturnedFields());

                        //todo: decide which id to put or type. or maby its ok this way. just need to doc.
                        InternalSearchHit searchHit = new InternalSearchHit(currentCombinedResults, hitFromFirstTable.id() + "|" + matchedHit.getId(), new StringText(hitFromFirstTable.getType() + "|" + matchedHit.getType()), hitFromFirstTable.getFields());
                        searchHit.sourceRef(hitFromFirstTable.getSourceRef());
                        searchHit.sourceAsMap().clear();
                        searchHit.sourceAsMap().putAll(hitFromFirstTable.sourceAsMap());

                        mergeSourceAndAddAliases(matchedHit.getSource(), searchHit, t1Alias, t2Alias);

                        combinedResults.add(searchHit);
                        currentCombinedResults++;
                        if(currentCombinedResults >= totalLimit) break;
                    }
                    if(currentCombinedResults >= totalLimit) break;

                }
                currentIndex += multiSearchSize;
                finishedMultiSearches = currentIndex >= hits.length-1;
            }


        }
        return combinedResults;
    }

    private void orderConditions(String t1Alias, String t2Alias) {
        Collection<Condition> conditions = nestedLoopsRequest.getT1FieldToCondition().values();
        for(Condition c : conditions){
            //TODO: support all orders and for each OPEAR find his related OPEAR (< is > , EQ is EQ ,etc..)
            if(!c.getName().startsWith(t2Alias+".") || !c.getValue().toString().startsWith(t1Alias +"."))
                throw new RuntimeException("On NestedLoops currently only supported Ordered conditions (t2.field2 OPEAR t1.field1) , badCondition was:" + c);
            c.setName(c.getName().replaceFirst(t2Alias+".",""));
            c.setValue(c.getValue().toString().replaceFirst(t1Alias+ ".", ""));
        }
    }


}
