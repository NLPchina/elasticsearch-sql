package org.elasticsearch.plugin.nlpcn.client;

import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.cluster.StateResponse;
import co.elastic.clients.elasticsearch.indices.Alias;
import co.elastic.clients.elasticsearch.indices.IndexSettings;
import co.elastic.clients.elasticsearch.indices.IndexState;
import co.elastic.clients.elasticsearch.nodes.info.NodeInfoHttp;
import co.elastic.clients.elasticsearch.nodes.info.NodeOperatingSystemInfo;
import co.elastic.clients.elasticsearch.nodes.info.NodeProcessInfo;
import co.elastic.clients.elasticsearch.nodes.info.NodeThreadPoolInfo;
import co.elastic.clients.json.JsonpMapper;
import co.elastic.clients.json.JsonpSerializable;
import jakarta.json.JsonObject;
import jakarta.json.JsonValue;
import jakarta.json.stream.JsonGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.Processors;
import org.elasticsearch.common.unit.RelativeByteSizeValue;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.health.metadata.HealthMetadata;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.ingest.IngestInfo;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.os.OsInfo;
import org.elasticsearch.monitor.process.ProcessInfo;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.ParsedComposite;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.ParsedFilter;
import org.elasticsearch.search.aggregations.bucket.filter.ParsedFilters;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.ParsedGeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.geogrid.ParsedGeoTileGrid;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.global.ParsedGlobal;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.ParsedDateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.ParsedHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.ParsedVariableWidthHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.VariableWidthHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.missing.MissingAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.missing.ParsedMissing;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.ParsedNested;
import org.elasticsearch.search.aggregations.bucket.nested.ParsedReverseNested;
import org.elasticsearch.search.aggregations.bucket.nested.ReverseNestedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.GeoDistanceAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.IpRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.ParsedBinaryRange;
import org.elasticsearch.search.aggregations.bucket.range.ParsedDateRange;
import org.elasticsearch.search.aggregations.bucket.range.ParsedGeoDistance;
import org.elasticsearch.search.aggregations.bucket.range.ParsedRange;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.sampler.InternalSampler;
import org.elasticsearch.search.aggregations.bucket.sampler.ParsedSampler;
import org.elasticsearch.search.aggregations.bucket.terms.DoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.LongRareTerms;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedDoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedLongRareTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedLongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedSignificantLongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedSignificantStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringRareTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantLongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringRareTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ExtendedStatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.GeoBoundsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.GeoCentroidAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalHDRPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.InternalHDRPercentiles;
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MedianAbsoluteDeviationAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedAvg;
import org.elasticsearch.search.aggregations.metrics.ParsedCardinality;
import org.elasticsearch.search.aggregations.metrics.ParsedExtendedStats;
import org.elasticsearch.search.aggregations.metrics.ParsedGeoBounds;
import org.elasticsearch.search.aggregations.metrics.ParsedGeoCentroid;
import org.elasticsearch.search.aggregations.metrics.ParsedHDRPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.ParsedHDRPercentiles;
import org.elasticsearch.search.aggregations.metrics.ParsedMax;
import org.elasticsearch.search.aggregations.metrics.ParsedMedianAbsoluteDeviation;
import org.elasticsearch.search.aggregations.metrics.ParsedMin;
import org.elasticsearch.search.aggregations.metrics.ParsedScriptedMetric;
import org.elasticsearch.search.aggregations.metrics.ParsedStats;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
import org.elasticsearch.search.aggregations.metrics.ParsedTDigestPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.ParsedTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.ParsedTopHits;
import org.elasticsearch.search.aggregations.metrics.ParsedValueCount;
import org.elasticsearch.search.aggregations.metrics.ParsedWeightedAvg;
import org.elasticsearch.search.aggregations.metrics.ScriptedMetricAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.StatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.WeightedAvgAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.ExtendedStatsBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.InternalBucketMetricValue;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.ParsedBucketMetricValue;
import org.elasticsearch.search.aggregations.pipeline.ParsedExtendedStatsBucket;
import org.elasticsearch.search.aggregations.pipeline.ParsedPercentilesBucket;
import org.elasticsearch.search.aggregations.pipeline.ParsedSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.ParsedStatsBucket;
import org.elasticsearch.search.aggregations.pipeline.PercentilesBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.StatsBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolInfo;
import org.elasticsearch.transport.TransportInfo;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.NamedXContentRegistry.Entry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Class to create Elasticsearch ActionResponse
 *
 * @author shiyuan
 * @version V1.0
 * @since 2022-12-21 10:01
 */
public class ResponseConverter {

    private static final Logger logger = LogManager.getLogger(ResponseConverter.class);

    private static final String EMPTY = "";
    private static final String KEY_CLUSTER_NAME = "cluster_name";
    private static final String KEY_VERSION = "version";
    private static final String KEY_STATE_UUID = "state_uuid";
    private static final String KEY_METADATA = "metadata";
    private static final String KEY_CLUSTER_COORDINATION = "cluster_coordination";
    private static final String KEY_SETTINGS = "settings";
    private static final String KEY_INDICES = "indices";
    private static final String KEK_HASHES_OF_CONSISTENT_SETTINGS = "hashes_of_consistent_settings";
    private static final String KEY_TEMPLATES = "templates";
    private static final String KEY_RESERVED_STATE = "reserved_state";
    private static final String KEY_CLUSTER_UUID = "cluster_uuid";
    private static final String KEY_CLUSTER_UUID_COMMITTED = "cluster_uuid_committed";
    private static final String KEY_IN_SYNC_ALLOCATIONS = "in_sync_allocations";
    private static final String KEY_MAPPING_VERSION = "mapping_version";
    private static final String KEY_SETTINGS_VERSION = "settings_version";
    private static final String KEY_ALIASES_VERSION = "aliases_version";
    private static final String KEY_ROUTING_NUM_SHARDS = "routing_num_shards";
    private static final String KEY_STATE = "state";
    private static final String KEY_MAPPINGS = "mappings";
    private static final String KEY_MAPPINGS_HASH = "mappings_hash";
    private static final String KEY_ALIASES = "aliases";
    private static final String KEY_ROLLOVER_INFOS = "rollover_info";
    private static final String KEY_WARMERS = "warmers";
    private static final String KEY_SYSTEM = "system";
    private static final String KEY_TIMESTAMP_RANGE = "timestamp_range";
    private static final String KEY_PRIMARY_TERMS = "primary_terms";
    private static final String KEY_NODES = "nodes";
    private static final String KEY_TRANSPORT_ADDRESS = "transport_address";
    private static final String KEY_ROLES = "roles";
    private static final String KEY_NAME = "name";
    private static final String KEY_EPHEMERAL = "ephemeral_id";
    private static final String KEY_ATTRIBUTES = "attributes";
    private static final String KEY_EXTERNAL_ID = "external_id";
    private static final String KEY_HIGH_WATERMARK = "high_watermark";
    private static final String KEY_HIGH_MAX_HEADROOM = "high_max_headroom";
    private static final String KEY_FLOOD_STAGE_WATERMARK = "flood_stage_watermark";
    private static final String KEY_FLOOD_STAGE_MAX_HEADROOM = "flood_stage_max_headroom";
    private static final String KEY_FROZEN_FLOOD_STAGE_WATERMARK = "frozen_flood_stage_watermark";
    private static final String KEY_FROZEN_FLOOD_STAGE_MAX_HEADROOM = "frozen_flood_stage_max_headroom";
    private static final String KEY_SHARD_LIMITS_TYPE = "shard_limits";
    private static final String KEY_MAX_SHARDS_PER_NODE = "max_shards_per_node";
    private static final String KEY_MAX_SHARDS_PER_NODE_FROZEN = "max_shards_per_node_frozen";
    private static final String KEY_KEEP_ALIVE = "keep_alive";

    private final JsonpMapper jsonpMapper;
    private final NamedXContentRegistry xContentRegistry;

    public ResponseConverter(JsonpMapper jsonpMapper) {
        this.jsonpMapper = jsonpMapper;
        this.xContentRegistry = new NamedXContentRegistry(Arrays.asList(
                //new Entry(Aggregation.class, new ParseField(AdjacencyMatrixAggregationBuilder.NAME), (p, c) -> ParsedAdjacencyMatrix.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(CompositeAggregationBuilder.NAME), (p, c) -> ParsedComposite.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(FilterAggregationBuilder.NAME), (p, c) -> ParsedFilter.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(FiltersAggregationBuilder.NAME), (p, c) -> ParsedFilters.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(GeoHashGridAggregationBuilder.NAME), (p, c) -> ParsedGeoHashGrid.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(GeoTileGridAggregationBuilder.NAME), (p, c) -> ParsedGeoTileGrid.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(GlobalAggregationBuilder.NAME), (p, c) -> ParsedGlobal.fromXContent(p, (String) c)),
                //new Entry(Aggregation.class, new ParseField(AutoDateHistogramAggregationBuilder.NAME), (p, c) -> ParsedAutoDateHistogram.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(DateHistogramAggregationBuilder.NAME), (p, c) -> ParsedDateHistogram.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(HistogramAggregationBuilder.NAME), (p, c) -> ParsedHistogram.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(VariableWidthHistogramAggregationBuilder.NAME), (p, c) -> ParsedVariableWidthHistogram.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(MissingAggregationBuilder.NAME), (p, c) -> ParsedMissing.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(NestedAggregationBuilder.NAME), (p, c) -> ParsedNested.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(ReverseNestedAggregationBuilder.NAME), (p, c) -> ParsedReverseNested.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(IpRangeAggregationBuilder.NAME), (p, c) -> ParsedBinaryRange.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(DateRangeAggregationBuilder.NAME), (p, c) -> ParsedDateRange.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(GeoDistanceAggregationBuilder.NAME), (p, c) -> ParsedGeoDistance.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(RangeAggregationBuilder.NAME), (p, c) -> ParsedRange.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(InternalSampler.PARSER_NAME), (p, c) -> ParsedSampler.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(DoubleTerms.NAME), (p, c) -> ParsedDoubleTerms.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(LongRareTerms.NAME), (p, c) -> ParsedLongRareTerms.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(LongTerms.NAME), (p, c) -> ParsedLongTerms.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(SignificantLongTerms.NAME), (p, c) -> ParsedSignificantLongTerms.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(SignificantStringTerms.NAME), (p, c) -> ParsedSignificantStringTerms.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(StringRareTerms.NAME), (p, c) -> ParsedStringRareTerms.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(StringTerms.NAME), (p, c) -> ParsedStringTerms.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(AvgAggregationBuilder.NAME), (p, c) -> ParsedAvg.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(CardinalityAggregationBuilder.NAME), (p, c) -> ParsedCardinality.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(ExtendedStatsAggregationBuilder.NAME), (p, c) -> ParsedExtendedStats.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(GeoBoundsAggregationBuilder.NAME), (p, c) -> ParsedGeoBounds.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(GeoCentroidAggregationBuilder.NAME), (p, c) -> ParsedGeoCentroid.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(InternalHDRPercentileRanks.NAME), (p, c) -> ParsedHDRPercentileRanks.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(InternalHDRPercentiles.NAME), (p, c) -> ParsedHDRPercentiles.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(MaxAggregationBuilder.NAME), (p, c) -> ParsedMax.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(MedianAbsoluteDeviationAggregationBuilder.NAME), (p, c) -> ParsedMedianAbsoluteDeviation.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(MinAggregationBuilder.NAME), (p, c) -> ParsedMin.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(ScriptedMetricAggregationBuilder.NAME), (p, c) -> ParsedScriptedMetric.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(StatsAggregationBuilder.NAME), (p, c) -> ParsedStats.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(SumAggregationBuilder.NAME), (p, c) -> ParsedSum.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(InternalTDigestPercentileRanks.NAME), (p, c) -> ParsedTDigestPercentileRanks.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(InternalTDigestPercentiles.NAME), (p, c) -> ParsedTDigestPercentiles.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(TopHitsAggregationBuilder.NAME), (p, c) -> ParsedTopHits.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(ValueCountAggregationBuilder.NAME), (p, c) -> ParsedValueCount.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(WeightedAvgAggregationBuilder.NAME), (p, c) -> ParsedWeightedAvg.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(InternalBucketMetricValue.NAME), (p, c) -> ParsedBucketMetricValue.fromXContent(p, (String) c)),
                //new Entry(Aggregation.class, new ParseField(DerivativePipelineAggregationBuilder.NAME), (p, c) -> ParsedDerivative.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(ExtendedStatsBucketPipelineAggregationBuilder.NAME), (p, c) -> ParsedExtendedStatsBucket.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(PercentilesBucketPipelineAggregationBuilder.NAME), (p, c) -> ParsedPercentilesBucket.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(InternalSimpleValue.NAME), (p, c) -> ParsedSimpleValue.fromXContent(p, (String) c)),
                new Entry(Aggregation.class, new ParseField(StatsBucketPipelineAggregationBuilder.NAME), (p, c) -> ParsedStatsBucket.fromXContent(p, (String) c))
                //new Entry(Aggregation.class, new ParseField(TimeSeriesAggregationBuilder.NAME), (p, c) -> ParsedTimeSeries.fromXContent(p, (String) c))
        ));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public ClusterStateResponse clusterStateResponse(StateResponse stateResponse) throws IOException {
        JsonObject jsonObject = stateResponse.valueBody().toJson().asJsonObject();
        String cn = jsonObject.getString(KEY_CLUSTER_NAME, EMPTY);
        ClusterName clusterName = new ClusterName(cn);
        int version = jsonObject.getInt(KEY_VERSION, 0);
        String stateUUID = jsonObject.getString(KEY_STATE_UUID, EMPTY);
        JsonObject metadataJson = jsonObject.getJsonObject(KEY_METADATA);
        Metadata metadata = Objects.nonNull(metadataJson) ? parseJson(metadataJson.toString(), this::loadMetadataFromXContent) :
                Metadata.EMPTY_METADATA;
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        JsonObject nodes = jsonObject.getJsonObject(KEY_NODES);
        if (Objects.nonNull(nodes)) {
            for (Map.Entry<String, JsonValue> entry : nodes.entrySet()) {
                JsonObject node = entry.getValue().asJsonObject();
                TransportAddress address = parseAddress(node.getString(KEY_TRANSPORT_ADDRESS));
                Set<DiscoveryNodeRole> roles = node.getJsonArray(KEY_ROLES).stream().map(e -> DiscoveryNodeRole.maybeGetRoleFromRoleName(e.toString()).orElse(null)).filter(Objects::nonNull).collect(Collectors.toSet());
                nodesBuilder.add(new DiscoveryNode(node.getString(KEY_NAME),
                        entry.getKey(),
                        node.getString(KEY_EPHEMERAL),
                        address.address().getHostString(),
                        address.getAddress(),
                        address,
                        (Map) node.getJsonObject(KEY_ATTRIBUTES),
                        roles,
                        Version.fromId(version),
                        node.getString(KEY_EXTERNAL_ID)));
            }
        }
        Map<String, ClusterState.Custom> customs = new HashMap<>(5);
        JsonObject health = jsonObject.getJsonObject(HealthMetadata.TYPE);
        if (Objects.nonNull(health)) {
            JsonObject disk = health.getJsonObject(HealthMetadata.Disk.TYPE);
            JsonObject shardLimits = health.getJsonObject(KEY_SHARD_LIMITS_TYPE);
            customs.put(HealthMetadata.TYPE, new HealthMetadata(new HealthMetadata.Disk(
                    RelativeByteSizeValue.parseRelativeByteSizeValue(disk.getString(KEY_HIGH_WATERMARK), KEY_HIGH_WATERMARK),
                    ByteSizeValue.parseBytesSizeValue(disk.getString(KEY_HIGH_MAX_HEADROOM), KEY_HIGH_MAX_HEADROOM),
                    RelativeByteSizeValue.parseRelativeByteSizeValue(disk.getString(KEY_FLOOD_STAGE_WATERMARK), KEY_FLOOD_STAGE_WATERMARK),
                    ByteSizeValue.parseBytesSizeValue(disk.getString(KEY_FLOOD_STAGE_MAX_HEADROOM), KEY_FLOOD_STAGE_MAX_HEADROOM),
                    RelativeByteSizeValue.parseRelativeByteSizeValue(disk.getString(KEY_FROZEN_FLOOD_STAGE_WATERMARK), KEY_FROZEN_FLOOD_STAGE_WATERMARK),
                    ByteSizeValue.parseBytesSizeValue(disk.getString(KEY_FROZEN_FLOOD_STAGE_MAX_HEADROOM), KEY_FROZEN_FLOOD_STAGE_MAX_HEADROOM)),
                    new HealthMetadata.ShardLimits(shardLimits.getInt(KEY_MAX_SHARDS_PER_NODE), shardLimits.getInt(KEY_MAX_SHARDS_PER_NODE_FROZEN))));
        }
        RoutingTable routingTable = RoutingTable.EMPTY_ROUTING_TABLE;
        ClusterBlocks blocks = ClusterBlocks.EMPTY_CLUSTER_BLOCK;
        RoutingNodes routingNodes = RoutingNodes.immutable(RoutingTable.EMPTY_ROUTING_TABLE, DiscoveryNodes.EMPTY_NODES);
        return new ClusterStateResponse(clusterName,
                new ClusterState(clusterName, version, stateUUID, metadata, routingTable, nodesBuilder.build(), Collections.emptyMap(), blocks, customs, false, routingNodes),
                false);
    }

    public NodesInfoResponse nodesInfoResponse(co.elastic.clients.elasticsearch.nodes.NodesInfoResponse nodesInfoResponse) throws IOException {
        Map<String, co.elastic.clients.elasticsearch.nodes.info.NodeInfo> nodeInfoMap = nodesInfoResponse.nodes();
        List<NodeInfo> nodes = new ArrayList<>();
        if (Objects.nonNull(nodeInfoMap)) {
            for (Map.Entry<String, co.elastic.clients.elasticsearch.nodes.info.NodeInfo> entry : nodeInfoMap.entrySet()) {
                co.elastic.clients.elasticsearch.nodes.info.NodeInfo nodeInfo = entry.getValue();
                Version version = Version.fromString(nodeInfo.version());
                Build build = new Build(Build.Type.fromDisplayName(nodeInfo.buildType(), false),
                        nodeInfo.buildHash(), null, false, nodeInfo.version());
                Set<DiscoveryNodeRole> roles = Optional.ofNullable(nodeInfo.roles()).orElse(Collections.emptyList()).stream().map(e -> DiscoveryNodeRole.maybeGetRoleFromRoleName(e.jsonValue()).orElse(null)).filter(Objects::nonNull).collect(Collectors.toSet());
                DiscoveryNode node = new DiscoveryNode(nodeInfo.name(), entry.getKey(),
                        parseAddress(nodeInfo.transportAddress()),
                        nodeInfo.attributes(), roles, version);
                Settings settings = parseJson(nodeInfo.settings(), Settings::fromXContent);
                OsInfo os = null;
                NodeOperatingSystemInfo systemInfo = nodeInfo.os();
                if (Objects.nonNull(systemInfo)) {
                    Integer allocatedProcessors = systemInfo.allocatedProcessors();
                    os = new OsInfo(systemInfo.refreshIntervalInMillis(), systemInfo.availableProcessors(),
                            Processors.of(Objects.nonNull(allocatedProcessors) ? allocatedProcessors.doubleValue() : null),
                            systemInfo.name(),
                            systemInfo.prettyName(),
                            systemInfo.arch(),
                            systemInfo.version());
                }
                ProcessInfo process = null;
                NodeProcessInfo processInfo = nodeInfo.process();
                if (Objects.nonNull(processInfo)) {
                    process = new ProcessInfo(processInfo.id(), processInfo.mlockall(), processInfo.refreshIntervalInMillis());
                }
                JvmInfo jvm = null;
                ThreadPoolInfo threadPool = null;
                Map<String, NodeThreadPoolInfo> threadPoolInfoMap = nodeInfo.threadPool();
                if (Objects.nonNull(threadPoolInfoMap)) {
                    List<ThreadPool.Info> infos = new ArrayList<>(threadPoolInfoMap.size());
                    for (Map.Entry<String, NodeThreadPoolInfo> infoEntry : threadPoolInfoMap.entrySet()) {
                        NodeThreadPoolInfo threadPoolInfo = infoEntry.getValue();
                        Time keepAlive = threadPoolInfo.keepAlive();
                        infos.add(new ThreadPool.Info(
                                infoEntry.getKey(),
                                ThreadPool.ThreadPoolType.fromType(threadPoolInfo.type()),
                                Optional.ofNullable(threadPoolInfo.core()).orElse(-1),
                                Optional.ofNullable(threadPoolInfo.max()).orElse(-1),
                                Objects.nonNull(keepAlive) ? TimeValue.parseTimeValue(keepAlive.time(), KEY_KEEP_ALIVE) : null,
                                threadPoolInfo.queueSize() >= 0 ? SizeValue.parseSizeValue(String.valueOf(threadPoolInfo.queueSize())) : null));
                    }
                    threadPool = new ThreadPoolInfo(infos);
                }
                TransportInfo transport = null;
                HttpInfo http = null;
                NodeInfoHttp nodeInfoHttp = nodeInfo.http();
                if (Objects.nonNull(nodeInfoHttp)) {
                    List<String> boundAddress = nodeInfoHttp.boundAddress();
                    int size = boundAddress.size();
                    TransportAddress[] boundAddressArr = new TransportAddress[size];
                    for (int i = 0; i < size; i++) {
                        boundAddressArr[i] = parseAddress(boundAddress.get(i));
                    }
                    http = new HttpInfo(new BoundTransportAddress(boundAddressArr, parseAddress(nodeInfoHttp.publishAddress())),
                            nodeInfoHttp.maxContentLengthInBytes());
                }
                PluginsAndModules plugins = null;
                IngestInfo ingest = null;
                AggregationInfo aggsInfo = null;
                ByteSizeValue totalIndexingBuffer = null;
                if (Objects.nonNull(nodeInfo.totalIndexingBuffer())) {
                    totalIndexingBuffer = ByteSizeValue.ofBytes(nodeInfo.totalIndexingBuffer());
                }
                nodes.add(new NodeInfo(version, TransportVersion.current(), build, node, settings, os, process, jvm, threadPool, transport, http, null, plugins, ingest, aggsInfo, totalIndexingBuffer));
            }
        }
        return new NodesInfoResponse(new ClusterName(nodesInfoResponse.clusterName()), nodes, Collections.emptyList());
    }

    public GetIndexResponse getIndexResponse(co.elastic.clients.elasticsearch.indices.GetIndexResponse getIndexResponse) throws IOException {
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

    public <T extends JsonpSerializable, R> R parseJson(T response, CheckedFunction<XContentParser, R, IOException> xContentParserCallback) throws IOException {
        String json = toJson(response);
        return parseJson(json, xContentParserCallback);
    }

    private <R> R parseJson(String json, CheckedFunction<XContentParser, R, IOException> xContentParserCallback) throws IOException {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY.withRegistry(this.xContentRegistry).withDeprecationHandler(LoggingDeprecationHandler.INSTANCE), json)) {
            return xContentParserCallback.apply(parser);
        }
    }

    private <T extends JsonpSerializable> String toJson(T object) {
        String json = "{}";
        if (Objects.nonNull(object)) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (JsonGenerator generator = this.jsonpMapper.jsonProvider().createGenerator(baos)) {
                object.serialize(generator, this.jsonpMapper);
            }
            try {
                json = baos.toString(StandardCharsets.UTF_8.name());
            } catch (UnsupportedEncodingException e) {
                logger.warn("could not read json", e);
            }
        }
        return json;
    }

    private TransportAddress parseAddress(String address) throws UnknownHostException {
        int lastIndexOf = address.lastIndexOf(":");
        return new TransportAddress(InetAddress.getByName(address.substring(0, lastIndexOf)), Integer.parseInt(address.substring(lastIndexOf + 1)));
    }

    private Metadata loadMetadataFromXContent(XContentParser parser) throws IOException {
        Metadata.Builder builder = new Metadata.Builder();

        XContentParser.Token token = parser.nextToken();
        String currentFieldName = parser.currentName();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                switch (currentFieldName) {
                    case KEY_CLUSTER_COORDINATION:
                        builder.coordinationMetadata(CoordinationMetadata.fromXContent(parser));
                        break;
                    case KEY_SETTINGS:
                        builder.persistentSettings(Settings.fromXContent(parser));
                        break;
                    case KEY_INDICES:
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            builder.put(loadIndexMetadataFromXContent(parser), false);
                        }
                        break;
                    case KEK_HASHES_OF_CONSISTENT_SETTINGS:
                        builder.hashesOfConsistentSettings(parser.mapStrings());
                        break;
                    case KEY_TEMPLATES:
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            builder.put(IndexTemplateMetadata.Builder.fromXContent(parser, parser.currentName()));
                        }
                        break;
                    case KEY_RESERVED_STATE:
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            builder.put(ReservedStateMetadata.fromXContent(parser));
                        }
                        break;
                    default:
                        logger.warn("Skipping custom object with type {}", currentFieldName);
                        parser.skipChildren();
                        break;
                }
            } else if (token.isValue()) {
                if (KEY_VERSION.equals(currentFieldName)) {
                    builder.version(parser.longValue());
                } else if (KEY_CLUSTER_UUID.equals(currentFieldName) || "uuid".equals(currentFieldName)) {
                    builder.clusterUUID(parser.text());
                } else if (KEY_CLUSTER_UUID_COMMITTED.equals(currentFieldName)) {
                    builder.clusterUUIDCommitted(parser.booleanValue());
                } else {
                    throw new IllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                }
            } else {
                throw new IllegalArgumentException("Unexpected token " + token);
            }
        }

        return builder.build();
    }

    public IndexMetadata loadIndexMetadataFromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            parser.nextToken();
        }
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
        IndexMetadata.Builder builder = new IndexMetadata.Builder(parser.currentName());

        String currentFieldName;
        XContentParser.Token token = parser.nextToken();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        while ((currentFieldName = parser.nextFieldName()) != null) {
            token = parser.nextToken();
            if (token == XContentParser.Token.START_OBJECT) {
                switch (currentFieldName) {
                    case KEY_SETTINGS:
                        builder.settings(Settings.fromXContent(parser));
                        break;
                    case KEY_MAPPINGS:
                        while ((currentFieldName = parser.nextFieldName()) != null) {
                            token = parser.nextToken();
                            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
                            Map<String, Object> mapping = new HashMap<>(1);
                            mapping.put(currentFieldName, parser.mapOrdered());
                            builder.putMapping(new MappingMetadata(currentFieldName, mapping));
                        }
                        break;
                    case KEY_ALIASES:
                        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                            builder.putAlias(AliasMetadata.Builder.fromXContent(parser));
                        }
                        break;
                    case KEY_IN_SYNC_ALLOCATIONS:
                        while ((currentFieldName = parser.nextFieldName()) != null) {
                            token = parser.nextToken();
                            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, token, parser);
                            final int shardId = Integer.parseInt(currentFieldName);
                            Set<String> allocationIds = new HashSet<>();
                            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                if (token == XContentParser.Token.VALUE_STRING) {
                                    allocationIds.add(parser.text());
                                }
                            }
                            builder.putInSyncAllocationIds(shardId, allocationIds);
                        }
                        break;
                    case KEY_ROLLOVER_INFOS:
                        while ((currentFieldName = parser.nextFieldName()) != null) {
                            token = parser.nextToken();
                            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
                            builder.putRolloverInfo(RolloverInfo.parse(parser, currentFieldName));
                        }
                        break;
                    case KEY_WARMERS:
                        assert Version.CURRENT.major <= 5;
                        parser.skipChildren();
                        break;
                    case KEY_TIMESTAMP_RANGE:
                        builder.timestampRange(IndexLongFieldRange.fromXContent(parser));
                        break;
                    default:
                        // assume it's custom index metadata
                        builder.putCustom(currentFieldName, parser.mapStrings());
                        break;
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                switch (currentFieldName) {
                    case KEY_MAPPINGS:
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
                                builder.putMapping(new MappingMetadata(new CompressedXContent(parser.binaryValue())));
                            } else {
                                Map<String, Object> mapping = parser.mapOrdered();
                                if (mapping.size() == 1) {
                                    String mappingType = mapping.keySet().iterator().next();
                                    builder.putMapping(new MappingMetadata(mappingType, mapping));
                                }
                            }
                        }
                        break;
                    case KEY_PRIMARY_TERMS:
                        int i = 0;
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser);
                            builder.primaryTerm(i++, parser.longValue());
                        }
                        break;
                    case KEY_ALIASES:
                        logger.warn("Skipping aliases");
                        parser.skipChildren();
                        break;
                    default:
                        throw new IllegalArgumentException("Unexpected field for an array " + currentFieldName);
                }
            } else if (token.isValue()) {
                switch (currentFieldName) {
                    case KEY_STATE:
                        builder.state(IndexMetadata.State.fromString(parser.text()));
                        break;
                    case KEY_VERSION:
                        builder.version(parser.longValue());
                        break;
                    case KEY_MAPPING_VERSION:
                        builder.mappingVersion(parser.longValue());
                        break;
                    case KEY_SETTINGS_VERSION:
                        builder.settingsVersion(parser.longValue());
                        break;
                    case KEY_ALIASES_VERSION:
                        builder.aliasesVersion(parser.longValue());
                        break;
                    case KEY_ROUTING_NUM_SHARDS:
                        builder.setRoutingNumShards(parser.intValue());
                        break;
                    case KEY_SYSTEM:
                        builder.system(parser.booleanValue());
                        break;
                    case KEY_MAPPINGS_HASH:
                        logger.warn("Skipping mappings hash");
                        parser.skipChildren();
                        break;
                    default:
                        throw new IllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                }
            } else {
                throw new IllegalArgumentException("Unexpected token " + token);
            }
        }

        return builder.build();
    }
}
