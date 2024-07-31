package org.elasticsearch.plugin.nlpcn.client;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.google.common.collect.Maps;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.support.AbstractClient;
import org.elasticsearch.plugin.nlpcn.client.handler.ActionHandler;
import org.elasticsearch.plugin.nlpcn.client.handler.BulkActionHandler;
import org.elasticsearch.plugin.nlpcn.client.handler.ClusterStateActionHandler;
import org.elasticsearch.plugin.nlpcn.client.handler.ClusterUpdateSettingsActionHandler;
import org.elasticsearch.plugin.nlpcn.client.handler.CreateIndexActionHandler;
import org.elasticsearch.plugin.nlpcn.client.handler.DeleteByQueryActionHandler;
import org.elasticsearch.plugin.nlpcn.client.handler.DeleteIndexActionHandler;
import org.elasticsearch.plugin.nlpcn.client.handler.GetIndexActionHandler;
import org.elasticsearch.plugin.nlpcn.client.handler.MultiSearchActionHandler;
import org.elasticsearch.plugin.nlpcn.client.handler.NodesInfoActionHandler;
import org.elasticsearch.plugin.nlpcn.client.handler.PutMappingActionHandler;
import org.elasticsearch.plugin.nlpcn.client.handler.RefreshActionHandler;
import org.elasticsearch.plugin.nlpcn.client.handler.SearchActionHandler;
import org.elasticsearch.plugin.nlpcn.client.handler.SearchScrollActionHandler;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Objects;

/**
 * Implementation of {@link AbstractClient} using {@link ElasticsearchClient}
 *
 * @author shiyuan
 * @version V1.0
 * @since 2022-12-19 21:16
 */
public class ElasticsearchRestClient extends AbstractClient implements AutoCloseable {

    private final ElasticsearchClient client;
    private final Map<String, ActionHandler<ActionRequest, ?, ?, ActionResponse>> handlers = Maps.newHashMap();

    public ElasticsearchRestClient(ElasticsearchClient client) {
        super(null, null);

        this.client = client;
        registerHandler(client);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(ActionType<Response> action, Request request, ActionListener<Response> listener) {
        try {
            String name = action.name();
            ActionHandler<ActionRequest, ?, ?, ActionResponse> handler = handlers.get(name);
            if (Objects.isNull(handler)) {
                listener.onFailure(new UnsupportedOperationException("elasticsearch rest client doesn't support action[" + name + "]"));
                return;
            }

            ActionResponse response = handler.handle(request);
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

    protected void registerHandler(ElasticsearchClient client) {
        doRegisterHandler(new BulkActionHandler(client));
        doRegisterHandler(new ClusterStateActionHandler(client));
        doRegisterHandler(new ClusterUpdateSettingsActionHandler(client));
        doRegisterHandler(new CreateIndexActionHandler(client));
        doRegisterHandler(new DeleteByQueryActionHandler(client));
        doRegisterHandler(new DeleteIndexActionHandler(client));
        doRegisterHandler(new GetIndexActionHandler(client));
        doRegisterHandler(new MultiSearchActionHandler(client));
        doRegisterHandler(new NodesInfoActionHandler(client));
        doRegisterHandler(new PutMappingActionHandler(client));
        doRegisterHandler(new RefreshActionHandler(client));
        doRegisterHandler(new SearchActionHandler(client));
        doRegisterHandler(new SearchScrollActionHandler(client));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void doRegisterHandler(ActionHandler actionHandler) {
        handlers.put(actionHandler.getName(), actionHandler);
    }
}
