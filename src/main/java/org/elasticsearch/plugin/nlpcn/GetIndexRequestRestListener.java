package org.elasticsearch.plugin.nlpcn;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.support.RestBuilderListener;
import org.elasticsearch.search.warmer.IndexWarmersMetaData;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

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
        GetIndexRequest.Feature[] features = getIndexRequest.featuresAsEnums();
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
                    case WARMERS:
                        writeWarmers(getIndexResponse.warmers().get(index), builder, channel.request());
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
    private void writeAliases(List<AliasMetaData> aliases, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.ALIASES);
        if (aliases != null) {
            for (AliasMetaData alias : aliases) {
                AliasMetaData.Builder.toXContent(alias, builder, params);
            }
        }
        builder.endObject();
    }

    private void writeMappings(ImmutableOpenMap<String, MappingMetaData> mappings, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.MAPPINGS);
        if (mappings != null) {
            for (ObjectObjectCursor<String, MappingMetaData> typeEntry : mappings) {
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

    private void writeWarmers(List<IndexWarmersMetaData.Entry> warmers, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.WARMERS);
        if (warmers != null) {
            for (IndexWarmersMetaData.Entry warmer : warmers) {
                IndexWarmersMetaData.toXContent(warmer, builder, params);
            }
        }
        builder.endObject();
    }

    static class Fields {
        static final XContentBuilderString ALIASES = new XContentBuilderString("aliases");
        static final XContentBuilderString MAPPINGS = new XContentBuilderString("mappings");
        static final XContentBuilderString SETTINGS = new XContentBuilderString("settings");
        static final XContentBuilderString WARMERS = new XContentBuilderString("warmers");
    }
}