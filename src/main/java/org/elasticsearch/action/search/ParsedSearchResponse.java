/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.search.ParsedSearchHits;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.profile.ParsedSearchProfileResults;
import org.elasticsearch.search.profile.SearchProfileResults;
import org.elasticsearch.search.suggest.ParsedSuggest;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * A response of a search request.
 */
public class ParsedSearchResponse {

    private static final Field REF_COUNTED_FIELD;

    static {
        try {
            REF_COUNTED_FIELD = SearchResponse.class.getDeclaredField("refCounted");
            if (!REF_COUNTED_FIELD.isAccessible()) {
                REF_COUNTED_FIELD.setAccessible(true);
            }
        } catch (NoSuchFieldException e) {
            throw new IllegalStateException(e);
        }
    }

    public static SearchResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(Token.START_OBJECT, parser.nextToken(), parser);
        parser.nextToken();
        return innerFromXContent(parser);
    }

    public static SearchResponse innerFromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(Token.FIELD_NAME, parser.currentToken(), parser);
        String currentFieldName = parser.currentName();
        SearchHits hits = null;
        InternalAggregations aggs = null;
        Suggest suggest = null;
        SearchProfileResults profile = null;
        boolean timedOut = false;
        Boolean terminatedEarly = null;
        int numReducePhases = 1;
        long tookInMillis = -1;
        int successfulShards = -1;
        int totalShards = -1;
        int skippedShards = 0; // 0 for BWC
        String scrollId = null;
        String searchContextId = null;
        List<ShardSearchFailure> failures = new ArrayList<>();
        SearchResponse.Clusters clusters = SearchResponse.Clusters.EMPTY;
        for (Token token = parser.nextToken(); token != Token.END_OBJECT; token = parser.nextToken()) {
            if (token == Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (SearchResponse.SCROLL_ID.match(currentFieldName, parser.getDeprecationHandler())) {
                    scrollId = parser.text();
                } else if (SearchResponse.POINT_IN_TIME_ID.match(currentFieldName, parser.getDeprecationHandler())) {
                    searchContextId = parser.text();
                } else if (SearchResponse.TOOK.match(currentFieldName, parser.getDeprecationHandler())) {
                    tookInMillis = parser.longValue();
                } else if (SearchResponse.TIMED_OUT.match(currentFieldName, parser.getDeprecationHandler())) {
                    timedOut = parser.booleanValue();
                } else if (SearchResponse.TERMINATED_EARLY.match(currentFieldName, parser.getDeprecationHandler())) {
                    terminatedEarly = parser.booleanValue();
                } else if (SearchResponse.NUM_REDUCE_PHASES.match(currentFieldName, parser.getDeprecationHandler())) {
                    numReducePhases = parser.intValue();
                } else {
                    parser.skipChildren();
                }
            } else if (token == Token.START_OBJECT) {
                if (SearchHits.Fields.HITS.equals(currentFieldName)) {
                    hits = ParsedSearchHits.fromXContent(parser);
                } else if (InternalAggregations.AGGREGATIONS_FIELD.equals(currentFieldName)) {
                    aggs = InternalAggregations.fromXContent(parser);
                } else if (Suggest.NAME.equals(currentFieldName)) {
                    suggest = ParsedSuggest.fromXContent(parser);
                } else if (SearchProfileResults.PROFILE_FIELD.equals(currentFieldName)) {
                    profile = ParsedSearchProfileResults.fromXContent(parser);
                } else if (RestActions._SHARDS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    while ((token = parser.nextToken()) != Token.END_OBJECT) {
                        if (token == Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token.isValue()) {
                            if (RestActions.FAILED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                parser.intValue(); // we don't need it but need to consume it
                            } else if (RestActions.SUCCESSFUL_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                successfulShards = parser.intValue();
                            } else if (RestActions.TOTAL_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                totalShards = parser.intValue();
                            } else if (RestActions.SKIPPED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                skippedShards = parser.intValue();
                            } else {
                                parser.skipChildren();
                            }
                        } else if (token == Token.START_ARRAY) {
                            if (RestActions.FAILURES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                while (parser.nextToken() != Token.END_ARRAY) {
                                    failures.add(ShardSearchFailure.fromXContent(parser));
                                }
                            } else {
                                parser.skipChildren();
                            }
                        } else {
                            parser.skipChildren();
                        }
                    }
                } else if (SearchResponse.Clusters._CLUSTERS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    clusters = ParsedClusters.fromXContent(parser);
                } else {
                    parser.skipChildren();
                }
            }
        }
        return unpooled(new SearchResponse(
                hits,
                aggs,
                suggest,
                timedOut,
                terminatedEarly,
                profile,
                numReducePhases,
                scrollId,
                totalShards,
                successfulShards,
                skippedShards,
                tookInMillis,
                failures.toArray(ShardSearchFailure.EMPTY_ARRAY),
                clusters,
                searchContextId
        ));
    }

    private static SearchResponse unpooled(SearchResponse searchResponse) {
        searchResponse.decRef();
        try {
            REF_COUNTED_FIELD.set(searchResponse, RefCounted.ALWAYS_REFERENCED);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
        return searchResponse;
    }

    /**
     * Holds info about the clusters that the search was executed on: how many in total, how many of them were successful
     * and how many of them were skipped and further details in a Map of Cluster objects
     * (when doing a cross-cluster search).
     */
    public static final class ParsedClusters {

        public static SearchResponse.Clusters fromXContent(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.currentToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
            int total = -1;
            int successful = -1;
            int skipped = -1;
            int running = 0;    // 0 for BWC
            int partial = 0;    // 0 for BWC
            int failed = 0;     // 0 for BWC
            Map<String, SearchResponse.Cluster> clusterInfoMap = ConcurrentCollections.newConcurrentMap();
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (SearchResponse.Clusters.TOTAL_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        total = parser.intValue();
                    } else if (SearchResponse.Clusters.SUCCESSFUL_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        successful = parser.intValue();
                    } else if (SearchResponse.Clusters.SKIPPED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        skipped = parser.intValue();
                    } else if (SearchResponse.Clusters.RUNNING_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        running = parser.intValue();
                    } else if (SearchResponse.Clusters.PARTIAL_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        partial = parser.intValue();
                    } else if (SearchResponse.Clusters.FAILED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        failed = parser.intValue();
                    } else {
                        parser.skipChildren();
                    }
                } else if (token == Token.START_OBJECT) {
                    if (SearchResponse.Clusters.DETAILS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        String currentDetailsFieldName = null;
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                currentDetailsFieldName = parser.currentName();  // cluster alias
                            } else if (token == Token.START_OBJECT) {
                                SearchResponse.Cluster c = ParsedCluster.fromXContent(currentDetailsFieldName, parser);
                                clusterInfoMap.put(currentDetailsFieldName, c);
                            } else {
                                parser.skipChildren();
                            }
                        }
                    } else {
                        parser.skipChildren();
                    }
                } else {
                    parser.skipChildren();
                }
            }
            if (clusterInfoMap.isEmpty()) {
                assert running == 0 && partial == 0 && failed == 0
                        : "Non cross-cluster should have counter for running, partial and failed equal to 0";
                return new SearchResponse.Clusters(total, successful, skipped);
            } else {
                return new SearchResponse.Clusters(clusterInfoMap);
            }
        }
    }

    /**
     * Represents the search metadata about a particular cluster involved in a cross-cluster search.
     * The Cluster object can represent either the local cluster or a remote cluster.
     * For the local cluster, clusterAlias should be specified as RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.
     * Its XContent is put into the "details" section the "_clusters" entry in the SearchResponse.
     * This is an immutable class, so updates made during the search progress (especially important for async
     * CCS searches) must be done by replacing the Cluster object with a new one.
     * See the Clusters clusterInfo Map for details.
     */
    public static class ParsedCluster {

        public static SearchResponse.Cluster fromXContent(String clusterAlias, XContentParser parser) throws IOException {
            XContentParser.Token token = parser.currentToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);

            String clusterName = clusterAlias;
            if (clusterAlias.equals("(local)")) {
                clusterName = "";
            }
            String indexExpression = null;
            String status = "running";
            boolean timedOut = false;
            long took = -1L;
            // these are all from the _shards section
            int totalShards = -1;
            int successfulShards = -1;
            int skippedShards = -1;
            int failedShards = -1;
            List<ShardSearchFailure> failures = new ArrayList<>();

            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (SearchResponse.Cluster.INDICES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        indexExpression = parser.text();
                    } else if (SearchResponse.Cluster.STATUS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        status = parser.text();
                    } else if (SearchResponse.TIMED_OUT.match(currentFieldName, parser.getDeprecationHandler())) {
                        timedOut = parser.booleanValue();
                    } else if (SearchResponse.TOOK.match(currentFieldName, parser.getDeprecationHandler())) {
                        took = parser.longValue();
                    } else {
                        parser.skipChildren();
                    }
                } else if (RestActions._SHARDS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    while ((token = parser.nextToken()) != Token.END_OBJECT) {
                        if (token == Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token.isValue()) {
                            if (RestActions.FAILED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                failedShards = parser.intValue();
                            } else if (RestActions.SUCCESSFUL_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                successfulShards = parser.intValue();
                            } else if (RestActions.TOTAL_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                totalShards = parser.intValue();
                            } else if (RestActions.SKIPPED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                skippedShards = parser.intValue();
                            } else {
                                parser.skipChildren();
                            }
                        } else {
                            parser.skipChildren();
                        }
                    }
                } else if (token == Token.START_ARRAY) {
                    if (RestActions.FAILURES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        while (parser.nextToken() != Token.END_ARRAY) {
                            failures.add(ShardSearchFailure.fromXContent(parser));
                        }
                    } else {
                        parser.skipChildren();
                    }
                } else {
                    parser.skipChildren();
                }
            }

            Integer totalShardsFinal = totalShards == -1 ? null : totalShards;
            Integer successfulShardsFinal = successfulShards == -1 ? null : successfulShards;
            Integer skippedShardsFinal = skippedShards == -1 ? null : skippedShards;
            Integer failedShardsFinal = failedShards == -1 ? null : failedShards;
            TimeValue tookTimeValue = took == -1L ? null : new TimeValue(took);
            boolean skipUnavailable = SearchResponse.Cluster.SKIP_UNAVAILABLE_DEFAULT;  // skipUnavailable is not exposed to XContent, so just use default

            return new SearchResponse.Cluster(
                    clusterName,
                    indexExpression,
                    skipUnavailable,
                    SearchResponse.Cluster.Status.valueOf(status.toUpperCase(Locale.ROOT)),
                    totalShardsFinal,
                    successfulShardsFinal,
                    skippedShardsFinal,
                    failedShardsFinal,
                    failures,
                    tookTimeValue,
                    timedOut
            );
        }
    }
}
