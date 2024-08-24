package org.elasticsearch.plugin.nlpcn.client.handler;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch._types.WaitForActiveShards;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.indices.Alias;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.IndexSettings;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * CreateIndexActionHandler
 *
 * @author shiyuan
 * @version V1.0
 * @since 2024-08-04 21:53
 */
public class CreateIndexActionHandler extends ActionHandler<org.elasticsearch.action.admin.indices.create.CreateIndexRequest, CreateIndexRequest, co.elastic.clients.elasticsearch.indices.CreateIndexResponse, CreateIndexResponse> {

    public CreateIndexActionHandler(ElasticsearchClient client) {
        super(client);
    }

    @Override
    public String getName() {
        return TransportCreateIndexAction.TYPE.name();
    }

    @Override
    protected co.elastic.clients.elasticsearch.indices.CreateIndexResponse doHandle(CreateIndexRequest createIndexRequest) throws IOException {
        return client.indices().create(createIndexRequest);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    protected CreateIndexRequest convertRequest(org.elasticsearch.action.admin.indices.create.CreateIndexRequest createIndexRequest) throws IOException {
        CreateIndexRequest.Builder builder = new CreateIndexRequest.Builder();
        builder.index(createIndexRequest.index());
        builder.mappings(fromJson(createIndexRequest.mappings(), TypeMapping._DESERIALIZER));
        builder.settings(fromJson(createIndexRequest.settings().toString(), IndexSettings._DESERIALIZER));
        Optional.ofNullable(createIndexRequest.ackTimeout()).ifPresent(e -> builder.timeout(Time.of(t -> t.time(e.toString()))));
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

    @Override
    protected CreateIndexResponse convertResponse(co.elastic.clients.elasticsearch.indices.CreateIndexResponse createIndexResponse) throws IOException {
        return parseJson(createIndexResponse, CreateIndexResponse::fromXContent);
    }
}
