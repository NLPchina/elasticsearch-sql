package org.elasticsearch.plugin.nlpcn;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;

import java.io.IOException;
import java.util.List;

/**
 * Created by Eliran on 6/10/2015.
 */
public class GetIndexRequestRestListener extends RestBuilderListener<GetIndexResponse> {

    private GetIndexRequest getIndexRequest;

    public GetIndexRequestRestListener(RestChannel channel,GetIndexRequest getIndexRequest) {
        super(channel);
        this.getIndexRequest = getIndexRequest;
    }

    @Override
    public RestResponse buildResponse(GetIndexResponse getIndexResponse, XContentBuilder builder) throws Exception {
        GetIndexRequest.Feature[] features = getIndexRequest.features();
        String[] indices = getIndexResponse.indices();

        builder.startObject();
        for (String index : indices) {
            builder.startObject(index);
            for (GetIndexRequest.Feature feature : features) {
                switch (feature) {
                    case ALIASES:
                        writeAliases(getIndexResponse.aliases().get(index), builder, channel.request());
                        break;
                    case MAPPINGS:
                        writeMappings(getIndexResponse.mappings().get(index), builder, channel.request());
                        break;
                    case SETTINGS:
                        writeSettings(getIndexResponse.settings().get(index), builder, channel.request());
                        break;
                    default:
                        throw new IllegalStateException("feature [" + feature + "] is not valid");
                }
            }
            builder.endObject();

        }
        builder.endObject();

        return new BytesRestResponse(RestStatus.OK, builder);
    }
    private void writeAliases(List<AliasMetadata> aliases, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.ALIASES);
        if (aliases != null) {
            for (AliasMetadata alias : aliases) {
                AliasMetadata.Builder.toXContent(alias, builder, params);
            }
        }
        builder.endObject();
    }

    private void writeMappings(ImmutableOpenMap<String, MappingMetadata> mappings, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.MAPPINGS);
        if (mappings != null) {
            for (ObjectObjectCursor<String, MappingMetadata> typeEntry : mappings) {
                builder.field(typeEntry.key);
                builder.map(typeEntry.value.sourceAsMap());
            }
        }
        builder.endObject();
    }

    private void writeSettings(Settings settings, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.SETTINGS);
        settings.toXContent(builder, params);
        builder.endObject();
    }


    static class Fields {
        static final String ALIASES = "aliases";
        static final String MAPPINGS = "mappings";
        static final String SETTINGS = "settings";
        static final String WARMERS = "warmers";
    }
}