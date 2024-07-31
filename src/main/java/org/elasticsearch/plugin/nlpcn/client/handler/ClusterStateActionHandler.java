package org.elasticsearch.plugin.nlpcn.client.handler;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch.cluster.StateRequest;
import co.elastic.clients.elasticsearch.cluster.StateResponse;
import jakarta.json.JsonObject;
import jakarta.json.JsonValue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.cluster.ClusterFeatures;
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
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RelativeByteSizeValue;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.health.metadata.HealthMetadata;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ClusterStateActionHandler
 *
 * @author shiyuan
 * @version V1.0
 * @since 2024-08-04 20:27
 */
public class ClusterStateActionHandler extends ActionHandler<ClusterStateRequest, StateRequest, StateResponse, ClusterStateResponse> {

    private static final Logger logger = LogManager.getLogger(ClusterStateActionHandler.class);

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

    public ClusterStateActionHandler(ElasticsearchClient client) {
        super(client);
    }

    @Override
    public String getName() {
        return ClusterStateAction.NAME;
    }

    @Override
    protected StateResponse doHandle(StateRequest stateRequest) throws IOException {
        return client.cluster().state(stateRequest);
    }

    @Override
    protected StateRequest convertRequest(ClusterStateRequest clusterStateRequest) throws IOException {
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

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    protected ClusterStateResponse convertResponse(StateResponse stateResponse) throws IOException {
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
                        VersionInformation.inferVersions(Version.fromId(version)),
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
                new ClusterState(clusterName, version, stateUUID, metadata, routingTable, nodesBuilder.build(), Collections.emptyMap(), new ClusterFeatures(Collections.emptyMap()), blocks, customs, false, routingNodes),
                false);
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

    private IndexMetadata loadIndexMetadataFromXContent(XContentParser parser) throws IOException {
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
