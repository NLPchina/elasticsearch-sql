package org.elasticsearch.plugin.nlpcn.client.handler;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.indices.Alias;
import co.elastic.clients.elasticsearch.indices.GetIndexRequest;
import co.elastic.clients.elasticsearch.indices.IndexSettings;
import co.elastic.clients.elasticsearch.indices.IndexState;
import co.elastic.clients.elasticsearch.indices.get.Feature;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * GetIndexActionHandler
 *
 * @author shiyuan
 * @version V1.0
 * @since 2024-08-04 21:46
 */
public class GetIndexActionHandler extends ActionHandler<org.elasticsearch.action.admin.indices.get.GetIndexRequest, GetIndexRequest, co.elastic.clients.elasticsearch.indices.GetIndexResponse, GetIndexResponse> {

    public GetIndexActionHandler(ElasticsearchClient client) {
        super(client);
    }

    @Override
    public String getName() {
        return GetIndexAction.NAME;
    }

    @Override
    protected co.elastic.clients.elasticsearch.indices.GetIndexResponse doHandle(GetIndexRequest getIndexRequest) throws IOException {
        return client.indices().get(getIndexRequest);
    }

    @Override
    protected GetIndexRequest convertRequest(org.elasticsearch.action.admin.indices.get.GetIndexRequest getIndexRequest) throws IOException {
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

    @Override
    protected GetIndexResponse convertResponse(co.elastic.clients.elasticsearch.indices.GetIndexResponse getIndexResponse) throws IOException {
        Map<String, IndexState> indexStateMap = getIndexResponse.result();
        int size = indexStateMap.size();
        String[] indices = new String[size];
        Map<String, MappingMetadata> mappings = new HashMap<>(size);
        Map<String, List<AliasMetadata>> aliases = new HashMap<>(size);
        Map<String, Settings> settings = new HashMap<>(size);
        Map<String, Settings> defaultSettings = new HashMap<>(size);
        Map<String, String> dataStreams = new HashMap<>(size);
        int i = 0;
        for (Map.Entry<String, IndexState> entry : indexStateMap.entrySet()) {
            String index = entry.getKey();
            IndexState indexState = entry.getValue();
            indices[i++] = index;
            TypeMapping typeMapping = indexState.mappings();
            Map<String, Object> mapping = new HashMap<>(1);
            if (Objects.nonNull(typeMapping)) {
                mapping.put(MapperService.SINGLE_MAPPING_NAME, parseJson(typeMapping, XContentParser::mapOrdered));
            }
            mappings.put(index, new MappingMetadata(MapperService.SINGLE_MAPPING_NAME, mapping));
            Map<String, Alias> aliasMap = indexState.aliases();
            if (Objects.nonNull(aliasMap)) {
                List<AliasMetadata> aliasMetadataList = new ArrayList<>(aliasMap.size());
                for (Map.Entry<String, Alias> aliasEntry : aliasMap.entrySet()) {
                    aliasMetadataList.add(parseJson(String.format("{\"%s\":%s}", aliasEntry.getKey(), toJson(aliasEntry.getValue())), parser -> {
                        parser.nextToken();
                        parser.nextToken();
                        return AliasMetadata.Builder.fromXContent(parser);
                    }));
                }
                aliases.put(index, aliasMetadataList);
            }
            IndexSettings indexSettings = indexState.settings();
            settings.put(index, Objects.nonNull(indexSettings) ? parseJson(toJson(indexSettings), Settings::fromXContent) : Settings.EMPTY);
            IndexSettings defaults = indexState.defaults();
            defaultSettings.put(index, Objects.nonNull(defaults) ? parseJson(toJson(defaults), Settings::fromXContent) : Settings.EMPTY);
            dataStreams.put(index, indexState.dataStream());
        }
        return new GetIndexResponse(indices, mappings, aliases, settings, defaultSettings, dataStreams);
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
}
