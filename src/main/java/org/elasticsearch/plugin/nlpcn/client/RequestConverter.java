package org.elasticsearch.plugin.nlpcn.client;

import co.elastic.clients.elasticsearch._types.Conflicts;
import co.elastic.clients.elasticsearch._types.ExpandWildcard;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch._types.SearchType;
import co.elastic.clients.elasticsearch._types.Slices;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch._types.VersionType;
import co.elastic.clients.elasticsearch._types.WaitForActiveShards;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch._types.query_dsl.GeoDistanceQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.GeoValidationMethod;
import co.elastic.clients.elasticsearch.cluster.PutClusterSettingsRequest;
import co.elastic.clients.elasticsearch.cluster.StateRequest;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.DeleteByQueryRequest;
import co.elastic.clients.elasticsearch.core.MsearchRequest;
import co.elastic.clients.elasticsearch.core.ScrollRequest;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.CreateOperation;
import co.elastic.clients.elasticsearch.core.bulk.DeleteOperation;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import co.elastic.clients.elasticsearch.core.bulk.UpdateAction;
import co.elastic.clients.elasticsearch.core.bulk.UpdateOperation;
import co.elastic.clients.elasticsearch.core.msearch.MultisearchBody;
import co.elastic.clients.elasticsearch.core.msearch.MultisearchHeader;
import co.elastic.clients.elasticsearch.core.msearch.RequestItem;
import co.elastic.clients.elasticsearch.indices.Alias;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.elasticsearch.indices.GetIndexRequest;
import co.elastic.clients.elasticsearch.indices.IndexSettings;
import co.elastic.clients.elasticsearch.indices.PutMappingRequest;
import co.elastic.clients.elasticsearch.indices.RefreshRequest;
import co.elastic.clients.elasticsearch.indices.get.Feature;
import co.elastic.clients.elasticsearch.nodes.NodesInfoRequest;
import co.elastic.clients.json.DelegatingDeserializer;
import co.elastic.clients.json.JsonEnum;
import co.elastic.clients.json.JsonpDeserializer;
import co.elastic.clients.json.JsonpMapper;
import co.elastic.clients.json.ObjectDeserializer;
import jakarta.json.stream.JsonParser;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Class to create Elasticsearch request
 *
 * @author shiyuan
 * @version V1.0
 * @since 2022-12-20 12:52
 */
public class RequestConverter {

    static {
        try {
            Field field = JsonEnum.Deserializer.class.getDeclaredField("lookupTable");
            field.setAccessible(true);

            @SuppressWarnings("unchecked")
            Map<String, GeoValidationMethod> lookupTable = (Map<String, GeoValidationMethod>) field.get(GeoValidationMethod._DESERIALIZER);
            for (GeoValidationMethod geoValidationMethod : GeoValidationMethod.values()) {
                lookupTable.put(geoValidationMethod.jsonValue().toUpperCase(Locale.ROOT), geoValidationMethod);
            }
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalStateException(e);
        }

        ((ObjectDeserializer<?>) DelegatingDeserializer.unwrap(GeoDistanceQuery._DESERIALIZER)).add((builder, value) -> {
        }, JsonpDeserializer.stringDeserializer(), "ignore_unmapped");
    }

    private final JsonpMapper jsonpMapper;

    public RequestConverter(JsonpMapper jsonpMapper) {
        this.jsonpMapper = jsonpMapper;
    }

    public PutClusterSettingsRequest putClusterSettingsRequest(ClusterUpdateSettingsRequest clusterUpdateSettingsRequest) throws IOException {
        PutClusterSettingsRequest.Builder builder = new PutClusterSettingsRequest.Builder();
        try (Reader reader = new StringReader(Strings.toString(clusterUpdateSettingsRequest));
             JsonParser parser = this.jsonpMapper.jsonProvider().createParser(reader)) {
            builder.withJson(parser, this.jsonpMapper);
        }
        Optional.ofNullable(clusterUpdateSettingsRequest.masterNodeTimeout()).ifPresent(e -> builder.masterTimeout(Time.of(t -> t.time(e.toString()))));
        Optional.ofNullable(clusterUpdateSettingsRequest.timeout()).ifPresent(e -> builder.timeout(Time.of(t -> t.time(e.toString()))));
        return builder.build();
    }

    public StateRequest stateRequest(ClusterStateRequest clusterStateRequest) {
        StateRequest.Builder builder = new StateRequest.Builder();
        builder.index(Arrays.asList(clusterStateRequest.indices()));
        if (clusterStateRequest.routingTable()) {
            builder.metric(ClusterState.Metric.ROUTING_TABLE.toString());
        }
        if (clusterStateRequest.nodes()) {
            builder.metric(ClusterState.Metric.NODES.toString());
        }
        if (clusterStateRequest.metadata()) {
            builder.metric(ClusterState.Metric.METADATA.toString());
        }
        if (clusterStateRequest.blocks()) {
            builder.metric(ClusterState.Metric.BLOCKS.toString());
        }
        if (clusterStateRequest.customs()) {
            builder.metric(ClusterState.Metric.CUSTOMS.toString());
        }
        builder.local(clusterStateRequest.local());
        Optional.ofNullable(clusterStateRequest.masterNodeTimeout()).ifPresent(e -> builder.masterTimeout(Time.of(t -> t.time(e.toString()))));
        Optional.ofNullable(clusterStateRequest.waitForTimeout()).ifPresent(e -> builder.waitForTimeout(Time.of(t -> t.time(e.toString()))));
        builder.waitForMetadataVersion(clusterStateRequest.waitForMetadataVersion());
        Optional.ofNullable(clusterStateRequest.indicesOptions()).ifPresent(options -> {
            builder.allowNoIndices(options.allowNoIndices());
            builder.ignoreUnavailable(options.ignoreUnavailable());
            builder.expandWildcards(getExpandWildcard(options.expandWildcards()));
        });
        return builder.build();
    }

    public NodesInfoRequest nodesInfoRequest(org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest nodesInfoRequest) {
        NodesInfoRequest.Builder builder = new NodesInfoRequest.Builder();
        builder.metric(new ArrayList<>(nodesInfoRequest.requestedMetrics()));
        builder.nodeId(Arrays.asList(nodesInfoRequest.nodesIds()));
        Optional.ofNullable(nodesInfoRequest.timeout()).ifPresent(e -> builder.timeout(Time.of(t -> t.time(e.toString()))));
        return builder.build();
    }

    public DeleteIndexRequest deleteIndexRequest(org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest deleteIndexRequest) {
        DeleteIndexRequest.Builder builder = new DeleteIndexRequest.Builder();
        builder.index(Arrays.asList(deleteIndexRequest.indices()));
        Optional.ofNullable(deleteIndexRequest.masterNodeTimeout()).ifPresent(e -> builder.masterTimeout(Time.of(t -> t.time(e.toString()))));
        Optional.ofNullable(deleteIndexRequest.timeout()).ifPresent(e -> builder.timeout(Time.of(t -> t.time(e.toString()))));
        Optional.ofNullable(deleteIndexRequest.indicesOptions()).ifPresent(options -> {
            builder.allowNoIndices(options.allowNoIndices());
            builder.ignoreUnavailable(options.ignoreUnavailable());
            builder.expandWildcards(getExpandWildcard(options.expandWildcards()));
        });
        return builder.build();
    }

    public PutMappingRequest putMappingRequest(org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest putMappingRequest) throws IOException {
        PutMappingRequest.Builder builder = new PutMappingRequest.Builder();
        if (Strings.hasLength(putMappingRequest.source())) {
            try (Reader reader = new StringReader(putMappingRequest.source());
                 JsonParser parser = this.jsonpMapper.jsonProvider().createParser(reader)) {
                builder.withJson(parser, this.jsonpMapper);
            }
        }
        builder.index(Arrays.asList(putMappingRequest.indices()));
        builder.writeIndexOnly(putMappingRequest.writeIndexOnly());
        Optional.ofNullable(putMappingRequest.indicesOptions()).ifPresent(options -> {
            builder.allowNoIndices(options.allowNoIndices());
            builder.ignoreUnavailable(options.ignoreUnavailable());
            builder.expandWildcards(getExpandWildcard(options.expandWildcards()));
        });
        Optional.ofNullable(putMappingRequest.timeout()).ifPresent(e -> builder.timeout(Time.of(t -> t.time(e.toString()))));
        Optional.ofNullable(putMappingRequest.masterNodeTimeout()).ifPresent(e -> builder.masterTimeout(Time.of(t -> t.time(e.toString()))));
        return builder.build();
    }

    public GetIndexRequest getIndexRequest(org.elasticsearch.action.admin.indices.get.GetIndexRequest getIndexRequest) {
        GetIndexRequest.Builder builder = new GetIndexRequest.Builder();
        builder.index(Arrays.asList(getIndexRequest.indices()));
        builder.includeDefaults(getIndexRequest.includeDefaults());
        builder.local(getIndexRequest.local());
        for (org.elasticsearch.action.admin.indices.get.GetIndexRequest.Feature feature : getIndexRequest.features()) {
            builder.features(getFeature(feature));
        }
        Optional.ofNullable(getIndexRequest.indicesOptions()).ifPresent(options -> {
            builder.allowNoIndices(options.allowNoIndices());
            builder.ignoreUnavailable(options.ignoreUnavailable());
            builder.expandWildcards(getExpandWildcard(options.expandWildcards()));
        });
        Optional.ofNullable(getIndexRequest.masterNodeTimeout()).ifPresent(e -> builder.masterTimeout(Time.of(t -> t.time(e.toString()))));
        return builder.build();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public CreateIndexRequest createIndexRequest(org.elasticsearch.action.admin.indices.create.CreateIndexRequest createIndexRequest) throws IOException {
        CreateIndexRequest.Builder builder = new CreateIndexRequest.Builder();
        builder.index(createIndexRequest.index());
        builder.mappings(fromJson(createIndexRequest.mappings(), TypeMapping._DESERIALIZER));
        builder.settings(fromJson(createIndexRequest.settings().toString(), IndexSettings._DESERIALIZER));
        Optional.ofNullable(createIndexRequest.timeout()).ifPresent(e -> builder.timeout(Time.of(t -> t.time(e.toString()))));
        Optional.ofNullable(createIndexRequest.masterNodeTimeout()).ifPresent(e -> builder.masterTimeout(Time.of(t -> t.time(e.toString()))));
        ActiveShardCount activeShardCount = createIndexRequest.waitForActiveShards();
        if (Objects.nonNull(activeShardCount) && activeShardCount.value() > -1) {
            builder.waitForActiveShards(WaitForActiveShards.of(w -> w.count(activeShardCount.value())));
        }
        for (org.elasticsearch.action.admin.indices.alias.Alias alias : createIndexRequest.aliases()) {
            BytesReference bytesReference;
            try (XContentBuilder aliasBuilder = XContentFactory.jsonBuilder()) {
                aliasBuilder.startObject();
                alias.toXContent(aliasBuilder, ToXContent.EMPTY_PARAMS);
                aliasBuilder.endObject();
                bytesReference = BytesReference.bytes(aliasBuilder);
            }
            Tuple<XContentType, Map<String, Object>> tuple = XContentHelper.convertToMap(bytesReference, true, XContentType.JSON);
            for (Map.Entry<String, Object> entry : tuple.v2().entrySet()) {
                String key = entry.getKey();
                try (XContentBuilder mapBuilder = XContentFactory.jsonBuilder()) {
                    mapBuilder.map((Map) entry.getValue());
                    bytesReference = BytesReference.bytes(mapBuilder);
                }
                builder.aliases(key, fromJson(bytesReference.utf8ToString(), Alias._DESERIALIZER));
            }
        }
        return builder.build();
    }

    public RefreshRequest refreshRequest(org.elasticsearch.action.admin.indices.refresh.RefreshRequest refreshRequest) {
        RefreshRequest.Builder builder = new RefreshRequest.Builder();
        builder.index(Arrays.asList(refreshRequest.indices()));
        Optional.ofNullable(refreshRequest.indicesOptions()).ifPresent(options -> {
            builder.allowNoIndices(options.allowNoIndices());
            builder.ignoreUnavailable(options.ignoreUnavailable());
            builder.expandWildcards(getExpandWildcard(options.expandWildcards()));
        });
        return builder.build();
    }

    public BulkRequest bulkRequest(org.elasticsearch.action.bulk.BulkRequest bulkRequest) throws IOException {
        BulkRequest.Builder builder = new BulkRequest.Builder();
        for (DocWriteRequest<?> request : bulkRequest.requests()) {
            switch (request.opType()) {
                case INDEX:
                    IndexRequest indexRequest = (IndexRequest) request;
                    IndexOperation.Builder<Map<String, Object>> indexOperationBuilder = new IndexOperation.Builder<>();
                    indexOperationBuilder.id(indexRequest.id());
                    indexOperationBuilder.index(indexRequest.index());
                    indexOperationBuilder.routing(indexRequest.routing());
                    indexOperationBuilder.ifPrimaryTerm(indexRequest.ifPrimaryTerm());
                    indexOperationBuilder.ifSeqNo(indexRequest.ifSeqNo());
                    indexOperationBuilder.pipeline(indexRequest.getPipeline());
                    indexOperationBuilder.requireAlias(indexRequest.isRequireAlias());
                    indexOperationBuilder.version(indexRequest.version());
                    indexOperationBuilder.versionType(getVersionType(indexRequest.versionType()));
                    indexOperationBuilder.dynamicTemplates(indexRequest.getDynamicTemplates());
                    indexOperationBuilder.document(indexRequest.sourceAsMap());
                    builder.operations(BulkOperation.of(bo -> bo.index(indexOperationBuilder.build())));
                    break;
                case CREATE:
                    IndexRequest createRequest = (IndexRequest) request;
                    CreateOperation.Builder<Map<String, Object>> createOperationBuilder = new CreateOperation.Builder<>();
                    createOperationBuilder.id(createRequest.id());
                    createOperationBuilder.index(createRequest.index());
                    createOperationBuilder.routing(createRequest.routing());
                    createOperationBuilder.ifPrimaryTerm(createRequest.ifPrimaryTerm());
                    createOperationBuilder.ifSeqNo(createRequest.ifSeqNo());
                    createOperationBuilder.pipeline(createRequest.getPipeline());
                    createOperationBuilder.requireAlias(createRequest.isRequireAlias());
                    createOperationBuilder.version(createRequest.version());
                    createOperationBuilder.versionType(getVersionType(createRequest.versionType()));
                    createOperationBuilder.dynamicTemplates(createRequest.getDynamicTemplates());
                    createOperationBuilder.document(createRequest.sourceAsMap());
                    builder.operations(BulkOperation.of(bo -> bo.create(createOperationBuilder.build())));
                    break;
                case DELETE:
                    DeleteRequest deleteRequest = (DeleteRequest) request;
                    DeleteOperation.Builder deleteOperationBuilder = new DeleteOperation.Builder();
                    deleteOperationBuilder.id(deleteRequest.id());
                    deleteOperationBuilder.index(deleteRequest.index());
                    deleteOperationBuilder.routing(deleteRequest.routing());
                    deleteOperationBuilder.ifPrimaryTerm(deleteRequest.ifPrimaryTerm());
                    deleteOperationBuilder.ifSeqNo(deleteRequest.ifSeqNo());
                    deleteOperationBuilder.version(deleteRequest.version());
                    deleteOperationBuilder.versionType(getVersionType(deleteRequest.versionType()));
                    builder.operations(BulkOperation.of(bo -> bo.delete(deleteOperationBuilder.build())));
                    break;
                case UPDATE:
                    UpdateRequest updateRequest = (UpdateRequest) request;
                    UpdateOperation.Builder<Map<String, Object>, Map<String, Object>> updateOperationBuilder = new UpdateOperation.Builder<>();
                    updateOperationBuilder.id(updateRequest.id());
                    updateOperationBuilder.index(updateRequest.index());
                    updateOperationBuilder.routing(updateRequest.routing());
                    updateOperationBuilder.ifPrimaryTerm(updateRequest.ifPrimaryTerm());
                    updateOperationBuilder.ifSeqNo(updateRequest.ifSeqNo());
                    updateOperationBuilder.requireAlias(updateRequest.isRequireAlias());
                    updateOperationBuilder.retryOnConflict(updateRequest.retryOnConflict());
                    UpdateAction.Builder<Map<String, Object>, Map<String, Object>> updateActionBuilder = new UpdateAction.Builder<>();
                    try (Reader reader = new StringReader(Strings.toString(updateRequest));
                         JsonParser parser = this.jsonpMapper.jsonProvider().createParser(reader)) {
                        updateActionBuilder.withJson(parser, this.jsonpMapper);
                    }
                    updateOperationBuilder.action(updateActionBuilder.build());
                    builder.operations(BulkOperation.of(bo -> bo.update(updateOperationBuilder.build())));
                    break;
                default:
                    throw new IllegalArgumentException();
            }
        }
        Set<String> indices = bulkRequest.getIndices();
        if (!indices.isEmpty()) {
            builder.index(indices.iterator().next());
        }
        builder.refresh(getRefreshPolicy(bulkRequest.getRefreshPolicy()));
        builder.requireAlias(bulkRequest.requireAlias());
        builder.pipeline(bulkRequest.pipeline());
        builder.routing(bulkRequest.routing());
        Optional.ofNullable(bulkRequest.timeout()).ifPresent(e -> builder.timeout(Time.of(t -> t.time(e.toString()))));
        ActiveShardCount activeShardCount = bulkRequest.waitForActiveShards();
        if (Objects.nonNull(activeShardCount) && activeShardCount.value() > -1) {
            builder.waitForActiveShards(WaitForActiveShards.of(w -> w.count(activeShardCount.value())));
        }
        return builder.build();
    }

    public SearchRequest searchRequest(org.elasticsearch.action.search.SearchRequest searchRequest) throws IOException {
        SearchRequest.Builder builder = new SearchRequest.Builder();
        try (Reader reader = new StringReader(Strings.toString(searchRequest.source()));
             JsonParser parser = this.jsonpMapper.jsonProvider().createParser(reader)) {
            builder.withJson(parser, this.jsonpMapper);
        }
        builder.index(Arrays.asList(searchRequest.indices()));
        builder.requestCache(searchRequest.requestCache());
        builder.allowPartialSearchResults(searchRequest.allowPartialSearchResults());
        builder.ccsMinimizeRoundtrips(searchRequest.isCcsMinimizeRoundtrips());
        builder.preference(searchRequest.preference());
        builder.routing(searchRequest.routing());
        builder.searchType(getSearchType(searchRequest.searchType()));
        builder.batchedReduceSize((long) searchRequest.getBatchedReduceSize());
        builder.maxConcurrentShardRequests((long) searchRequest.getMaxConcurrentShardRequests());
        Optional.ofNullable(searchRequest.minCompatibleShardNode()).ifPresent(e -> builder.minCompatibleShardNode(e.toString()));
        Optional.ofNullable(searchRequest.scroll()).ifPresent(e -> builder.scroll(Time.of(t -> t.time(e.keepAlive().toString()))));
        Optional.ofNullable(searchRequest.indicesOptions()).ifPresent(options -> {
            builder.allowNoIndices(options.allowNoIndices());
            builder.ignoreUnavailable(options.ignoreUnavailable());
            builder.expandWildcards(getExpandWildcard(options.expandWildcards()));
        });
        return builder.build();
    }

    public ScrollRequest scrollRequest(SearchScrollRequest searchScrollRequest) {
        ScrollRequest.Builder builder = new ScrollRequest.Builder();
        builder.scrollId(searchScrollRequest.scrollId());
        Optional.ofNullable(searchScrollRequest.scroll()).ifPresent(e -> builder.scroll(Time.of(t -> t.time(e.keepAlive().toString()))));
        return builder.build();
    }

    public MsearchRequest msearchRequest(MultiSearchRequest multiSearchRequest) throws IOException {
        MsearchRequest.Builder builder = new MsearchRequest.Builder();
        if (multiSearchRequest.maxConcurrentSearchRequests() > 0) {
            builder.maxConcurrentSearches((long) multiSearchRequest.maxConcurrentSearchRequests());
        }
        for (org.elasticsearch.action.search.SearchRequest request : multiSearchRequest.requests()) {
            MultisearchHeader.Builder msearchHeaderBuilder = new MultisearchHeader.Builder();
            msearchHeaderBuilder.index(Arrays.asList(request.indices()));
            msearchHeaderBuilder.requestCache(request.requestCache());
            msearchHeaderBuilder.allowPartialSearchResults(request.allowPartialSearchResults());
            msearchHeaderBuilder.ccsMinimizeRoundtrips(request.isCcsMinimizeRoundtrips());
            msearchHeaderBuilder.preference(request.preference());
            msearchHeaderBuilder.routing(request.routing());
            msearchHeaderBuilder.searchType(getSearchType(request.searchType()));
            Optional.ofNullable(request.indicesOptions()).ifPresent(options -> {
                msearchHeaderBuilder.allowNoIndices(options.allowNoIndices());
                msearchHeaderBuilder.ignoreUnavailable(options.ignoreUnavailable());
                msearchHeaderBuilder.expandWildcards(getExpandWildcard(options.expandWildcards()));
                msearchHeaderBuilder.ignoreThrottled(options.ignoreThrottled());
            });

            MultisearchBody.Builder msearchBodyBuilder = new MultisearchBody.Builder();
            try (Reader reader = new StringReader(Strings.toString(request.source()));
                 JsonParser parser = this.jsonpMapper.jsonProvider().createParser(reader)) {
                msearchBodyBuilder.withJson(parser, this.jsonpMapper);
            }
            RequestItem.Builder requestItemBuilder = new RequestItem.Builder();
            requestItemBuilder.header(msearchHeaderBuilder.build());
            requestItemBuilder.body(msearchBodyBuilder.build());
            builder.searches(requestItemBuilder.build());
        }
        Optional.ofNullable(multiSearchRequest.indicesOptions()).ifPresent(options -> {
            builder.allowNoIndices(options.allowNoIndices());
            builder.ignoreUnavailable(options.ignoreUnavailable());
            builder.expandWildcards(getExpandWildcard(options.expandWildcards()));
        });
        return builder.build();
    }

    public DeleteByQueryRequest deleteByQueryRequest(org.elasticsearch.index.reindex.DeleteByQueryRequest deleteByQueryRequest) throws IOException {
        DeleteByQueryRequest.Builder builder = new DeleteByQueryRequest.Builder();
        org.elasticsearch.action.search.SearchRequest searchRequest = deleteByQueryRequest.getSearchRequest();
        if (Objects.nonNull(searchRequest)) {
            try (Reader reader = new StringReader(Strings.toString(deleteByQueryRequest));
                 JsonParser parser = this.jsonpMapper.jsonProvider().createParser(reader)) {
                builder.withJson(parser, this.jsonpMapper);
            }
            builder.requestCache(searchRequest.requestCache());
            builder.preference(searchRequest.preference());
            builder.searchType(getSearchType(searchRequest.searchType()));
        }
        builder.conflicts(deleteByQueryRequest.isAbortOnVersionConflict() ? Conflicts.Abort : Conflicts.Proceed);
        builder.index(Arrays.asList(deleteByQueryRequest.indices()));
        builder.routing(deleteByQueryRequest.getRouting());
        if (deleteByQueryRequest.getMaxDocs() > -1) {
            builder.maxDocs((long) deleteByQueryRequest.getMaxDocs());
        }
        builder.requestsPerSecond(deleteByQueryRequest.getRequestsPerSecond());
        builder.refresh(deleteByQueryRequest.isRefresh());
        ActiveShardCount waitForActiveShards = deleteByQueryRequest.getWaitForActiveShards();
        if (Objects.nonNull(waitForActiveShards) && waitForActiveShards.value() > -1) {
            builder.waitForActiveShards(WaitForActiveShards.of(w -> w.count(waitForActiveShards.value())));
        }
        builder.slices(Slices.of(s -> s.value(deleteByQueryRequest.getSlices())));
        Optional.ofNullable(deleteByQueryRequest.getScrollTime()).ifPresent(e -> builder.scroll(Time.of(t -> t.time(e.toString()))));
        Optional.ofNullable(deleteByQueryRequest.getTimeout()).ifPresent(e -> builder.timeout(Time.of(t -> t.time(e.toString()))));
        Optional.ofNullable(deleteByQueryRequest.indicesOptions()).ifPresent(options -> {
            builder.allowNoIndices(options.allowNoIndices());
            builder.ignoreUnavailable(options.ignoreUnavailable());
            builder.expandWildcards(getExpandWildcard(options.expandWildcards()));
        });
        return builder.build();
    }

    private <T> T fromJson(String json, JsonpDeserializer<T> deserializer) throws IOException {
        Objects.requireNonNull(json, "json must not be null");
        Objects.requireNonNull(deserializer, "deserializer must not be null");

        try (Reader reader = new StringReader(json);
             JsonParser parser = jsonpMapper.jsonProvider().createParser(reader)) {
            return deserializer.deserialize(parser, jsonpMapper);
        }
    }

    private List<ExpandWildcard> getExpandWildcard(EnumSet<IndicesOptions.WildcardStates> states) {
        List<ExpandWildcard> expandWildcards = new ArrayList<>();
        if (states.isEmpty()) {
            expandWildcards.add(ExpandWildcard.None);
        } else if (states.containsAll(EnumSet.allOf(IndicesOptions.WildcardStates.class))) {
            expandWildcards.add(ExpandWildcard.All);
        } else {
            for (IndicesOptions.WildcardStates state : states) {
                switch (state) {
                    case OPEN:
                        expandWildcards.add(ExpandWildcard.Open);
                        break;
                    case CLOSED:
                        expandWildcards.add(ExpandWildcard.Closed);
                        break;
                    case HIDDEN:
                        expandWildcards.add(ExpandWildcard.Hidden);
                        break;
                    default:
                        break;
                }
            }
        }
        return expandWildcards;
    }

    private Feature getFeature(org.elasticsearch.action.admin.indices.get.GetIndexRequest.Feature feature) {
        switch (feature) {
            case ALIASES:
                return Feature.Aliases;
            case MAPPINGS:
                return Feature.Mappings;
            case SETTINGS:
                return Feature.Settings;
            default:
                throw new IllegalArgumentException();
        }
    }

    private VersionType getVersionType(org.elasticsearch.index.VersionType versionType) {
        if (Objects.isNull(versionType)) {
            return null;
        }
        switch (versionType) {
            case INTERNAL:
                return VersionType.Internal;
            case EXTERNAL:
                return VersionType.External;
            case EXTERNAL_GTE:
                return VersionType.ExternalGte;
            default:
                throw new IllegalArgumentException();
        }
    }

    private Refresh getRefreshPolicy(WriteRequest.RefreshPolicy refreshPolicy) {
        if (Objects.isNull(refreshPolicy)) {
            return null;
        }
        switch (refreshPolicy) {
            case IMMEDIATE:
                return Refresh.True;
            case NONE:
                return Refresh.False;
            case WAIT_UNTIL:
                return Refresh.WaitFor;
            default:
                throw new IllegalArgumentException();
        }
    }

    private SearchType getSearchType(org.elasticsearch.action.search.SearchType searchType) {
        if (Objects.isNull(searchType)) {
            return null;
        }
        switch (searchType) {
            case QUERY_THEN_FETCH:
                return SearchType.QueryThenFetch;
            case DFS_QUERY_THEN_FETCH:
                return SearchType.DfsQueryThenFetch;
            default:
                throw new IllegalArgumentException();
        }
    }
}
