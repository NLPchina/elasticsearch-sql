package org.elasticsearch.plugin.nlpcn;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.AggregationQueryAction;
import org.nlpcn.es4sql.query.DefaultQueryAction;
import org.nlpcn.es4sql.query.DeleteQueryAction;
import org.nlpcn.es4sql.query.QueryAction;
import org.nlpcn.es4sql.query.ShowQueryAction;
import org.nlpcn.es4sql.query.SqlElasticRequestBuilder;
import org.nlpcn.es4sql.query.SqlElasticSearchRequestBuilder;
import org.nlpcn.es4sql.query.join.ESJoinQueryAction;
import org.nlpcn.es4sql.query.multi.MultiQueryAction;
import org.nlpcn.es4sql.query.multi.MultiQueryRequestBuilder;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by Eliran on 3/10/2015.
 */
public class QueryActionElasticExecutor {

    private static final Logger LOGGER = LogManager.getLogger();

    public static SearchResponse executeSearchAction(DefaultQueryAction searchQueryAction) throws SqlParseException {
        SqlElasticSearchRequestBuilder builder  =  searchQueryAction.explain();
        SearchResponse resp = (SearchResponse) builder.get();

        //
        if (resp.getFailedShards() > 0) {
            if (resp.getSuccessfulShards() < 1) {
                throw new IllegalStateException("fail to search[" + builder + "], " + Arrays.toString(resp.getShardFailures()));
            }

            LOGGER.warn("The failures that occurred during the search[{}]: {}", builder, Arrays.toString(resp.getShardFailures()));
        }

        return resp;
    }

    public static SearchHits executeJoinSearchAction(Client client , ESJoinQueryAction joinQueryAction) throws IOException, SqlParseException {
        SqlElasticRequestBuilder joinRequestBuilder = joinQueryAction.explain();
        ElasticJoinExecutor executor = ElasticJoinExecutor.createJoinExecutor(client,joinRequestBuilder);
        executor.run();
        return executor.getHits();
    }

    public static Aggregations executeAggregationAction(AggregationQueryAction aggregationQueryAction) throws SqlParseException {
        SqlElasticSearchRequestBuilder select =  aggregationQueryAction.explain();
        SearchResponse resp = (SearchResponse) select.get();

        //
        if (resp.getFailedShards() > 0) {
            if (resp.getSuccessfulShards() < 1) {
                throw new IllegalStateException("fail to aggregation[" + select + "], " + Arrays.toString(resp.getShardFailures()));
            }

            LOGGER.warn("The failures that occurred during the aggregation[{}]: {}", select, Arrays.toString(resp.getShardFailures()));
        }

        return resp.getAggregations();
    }

    public static ActionResponse executeDeleteAction(DeleteQueryAction deleteQueryAction) throws SqlParseException {
        return deleteQueryAction.explain().get();
    }

    public static SearchHits executeMultiQueryAction(Client client, MultiQueryAction queryAction) throws SqlParseException, IOException {
        SqlElasticRequestBuilder multiRequestBuilder = queryAction.explain();
        ElasticHitsExecutor executor = MultiRequestExecutorFactory.createExecutor(client, (MultiQueryRequestBuilder) multiRequestBuilder);
        executor.run();
        return executor.getHits();
    }

    public static Object executeAnyAction(Client client , QueryAction queryAction) throws SqlParseException, IOException {
        if(queryAction instanceof DefaultQueryAction)
            return executeSearchAction((DefaultQueryAction) queryAction);
        if(queryAction instanceof AggregationQueryAction)
            return executeAggregationAction((AggregationQueryAction) queryAction);
        if(queryAction instanceof ESJoinQueryAction)
            return executeJoinSearchAction(client, (ESJoinQueryAction) queryAction);
        if(queryAction instanceof MultiQueryAction)
            return executeMultiQueryAction(client, (MultiQueryAction) queryAction);
        if(queryAction instanceof DeleteQueryAction )
            return executeDeleteAction((DeleteQueryAction) queryAction);
        if (queryAction instanceof  ShowQueryAction)
            return ((ShowQueryAction)queryAction).explain().get();
        return null;
    }


}
