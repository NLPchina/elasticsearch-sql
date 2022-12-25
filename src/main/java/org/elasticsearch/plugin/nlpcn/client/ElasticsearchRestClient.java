package org.elasticsearch.plugin.nlpcn.client;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.JsonpMapper;
import co.elastic.clients.json.JsonpSerializable;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.support.AbstractClient;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

/**
 * Implementation of {@link AbstractClient} using {@link ElasticsearchClient}
 *
 * @author shiyuan
 * @version V1.0
 * @since 2022-12-19 21:16
 */
public class ElasticsearchRestClient extends AbstractClient {

    private final ElasticsearchClient client;
    private final RequestConverter requestConverter;
    private final ResponseConverter responseConverter;

    public ElasticsearchRestClient(ElasticsearchClient client) {
        super(null, null);

        this.client = client;
        JsonpMapper jsonpMapper = client._jsonpMapper();
        this.requestConverter = new RequestConverter(jsonpMapper);
        this.responseConverter = new ResponseConverter(jsonpMapper);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(ActionType<Response> action, Request request, ActionListener<Response> listener) {
        try {
            String name = action.name();
            ActionResponse response;
            switch (name) {
                case ClusterUpdateSettingsAction.NAME:
                    response = doExecute(client -> client.cluster().putSettings(requestConverter.putClusterSettingsRequest((ClusterUpdateSettingsRequest) request)),
                            r -> responseConverter.parseJson(r, ClusterUpdateSettingsResponse::fromXContent));
                    break;
                case ClusterStateAction.NAME:
                    response = doExecute(client -> client.cluster().state(requestConverter.stateRequest((ClusterStateRequest) request)),
                            responseConverter::clusterStateResponse);
                    break;
                case NodesInfoAction.NAME:
                    response = doExecute(client -> client.nodes().info(requestConverter.nodesInfoRequest((NodesInfoRequest) request)),
                            responseConverter::nodesInfoResponse);
                    break;
                case DeleteIndexAction.NAME:
                    response = doExecute(client -> client.indices().delete(requestConverter.deleteIndexRequest((DeleteIndexRequest) request)),
                            r -> responseConverter.parseJson(r, AcknowledgedResponse::fromXContent));
                    break;
                case PutMappingAction.NAME:
                    response = doExecute(client -> client.indices().putMapping(requestConverter.putMappingRequest((PutMappingRequest) request)),
                            r -> responseConverter.parseJson(r, AcknowledgedResponse::fromXContent));
                    break;
                case GetIndexAction.NAME:
                    response = doExecute(client -> client.indices().get(requestConverter.getIndexRequest((GetIndexRequest) request)),
                            responseConverter::getIndexResponse);
                    break;
                case CreateIndexAction.NAME:
                    response = doExecute(client -> client.indices().create(requestConverter.createIndexRequest((CreateIndexRequest) request)),
                            r -> responseConverter.parseJson(r, CreateIndexResponse::fromXContent));
                    break;
                case RefreshAction.NAME:
                    response = doExecute(client -> client.indices().refresh(requestConverter.refreshRequest((RefreshRequest) request)),
                            r -> responseConverter.parseJson(r, RefreshResponse::fromXContent));
                    break;
                case BulkAction.NAME:
                    response = doExecute(client -> client.bulk(requestConverter.bulkRequest((BulkRequest) request)),
                            r -> responseConverter.parseJson(r, BulkResponse::fromXContent));
                    break;
                case SearchAction.NAME:
                    response = doExecute(client -> client.search(requestConverter.searchRequest((SearchRequest) request), Object.class),
                            r -> responseConverter.parseJson(r, SearchResponse::fromXContent));
                    break;
                case SearchScrollAction.NAME:
                    response = doExecute(client -> client.scroll(requestConverter.scrollRequest((SearchScrollRequest) request), Object.class),
                            r -> responseConverter.parseJson(r, SearchResponse::fromXContent));
                    break;
                case MultiSearchAction.NAME:
                    response = doExecute(client -> client.msearch(requestConverter.msearchRequest((MultiSearchRequest) request), Object.class),
                            r -> responseConverter.parseJson(r, MultiSearchResponse::fromXContext));
                    break;
                case DeleteByQueryAction.NAME:
                    response = doExecute(client -> client.deleteByQuery(requestConverter.deleteByQueryRequest((DeleteByQueryRequest) request)),
                            r -> responseConverter.parseJson(r, BulkByScrollResponse::fromXContent));
                    break;
                default:
                    listener.onFailure(new UnsupportedOperationException("elasticsearch rest client doesn't support action[" + name + "]"));
                    return;
            }

            listener.onResponse((Response) response);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public void close() {
        try {
            client._transport().close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public <T extends JsonpSerializable, R extends ActionResponse> R doExecute(CheckedFunction<ElasticsearchClient, T, IOException> clientCallback, CheckedFunction<T, R, IOException> responseCallback) throws IOException {
        Objects.requireNonNull(clientCallback, "clientCallback must not be null");
        Objects.requireNonNull(responseCallback, "responseCallback must not be null");

        T response = clientCallback.apply(client);
        return responseCallback.apply(response);
    }
}
