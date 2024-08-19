package org.elasticsearch.plugin.nlpcn.client.handler;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch._types.VersionType;
import co.elastic.clients.elasticsearch._types.WaitForActiveShards;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.CreateOperation;
import co.elastic.clients.elasticsearch.core.bulk.DeleteOperation;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import co.elastic.clients.elasticsearch.core.bulk.UpdateAction;
import co.elastic.clients.elasticsearch.core.bulk.UpdateOperation;
import jakarta.json.stream.JsonParser;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.ParsedBulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.Strings;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * BulkActionHandler
 *
 * @author shiyuan
 * @version V1.0
 * @since 2024-08-04 22:06
 */
public class BulkActionHandler extends ActionHandler<org.elasticsearch.action.bulk.BulkRequest, BulkRequest, co.elastic.clients.elasticsearch.core.BulkResponse, BulkResponse> {

    public BulkActionHandler(ElasticsearchClient client) {
        super(client);
    }

    @Override
    public String getName() {
        return TransportBulkAction.NAME;
    }

    @Override
    protected co.elastic.clients.elasticsearch.core.BulkResponse doHandle(BulkRequest bulkRequest) throws IOException {
        return client.bulk(bulkRequest);
    }

    @Override
    protected BulkRequest convertRequest(org.elasticsearch.action.bulk.BulkRequest bulkRequest) throws IOException {
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

    @Override
    protected BulkResponse convertResponse(co.elastic.clients.elasticsearch.core.BulkResponse bulkResponse) throws IOException {
        return parseJson(bulkResponse, ParsedBulkResponse::fromXContent);
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
}
