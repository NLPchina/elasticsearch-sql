package org.elasticsearch.plugin.nlpcn.client.handler;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch.nodes.NodesInfoRequest;
import co.elastic.clients.elasticsearch.nodes.info.NodeInfo;
import co.elastic.clients.elasticsearch.nodes.info.NodeInfoHttp;
import co.elastic.clients.elasticsearch.nodes.info.NodeOperatingSystemInfo;
import co.elastic.clients.elasticsearch.nodes.info.NodeProcessInfo;
import co.elastic.clients.elasticsearch.nodes.info.NodeThreadPoolInfo;
import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.action.admin.cluster.node.info.TransportNodesInfoAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.Processors;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.ingest.IngestInfo;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.os.OsInfo;
import org.elasticsearch.monitor.process.ProcessInfo;
import org.elasticsearch.search.aggregations.support.AggregationInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolInfo;
import org.elasticsearch.transport.TransportInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * NodesInfoActionHandler
 *
 * @author shiyuan
 * @version V1.0
 * @since 2024-08-04 21:09
 */
public class NodesInfoActionHandler extends ActionHandler<org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest, NodesInfoRequest, co.elastic.clients.elasticsearch.nodes.NodesInfoResponse, NodesInfoResponse> {

    private static final String KEY_KEEP_ALIVE = "keep_alive";

    public NodesInfoActionHandler(ElasticsearchClient client) {
        super(client);
    }

    @Override
    public String getName() {
        return TransportNodesInfoAction.TYPE.name();
    }

    @Override
    protected co.elastic.clients.elasticsearch.nodes.NodesInfoResponse doHandle(NodesInfoRequest nodesInfoRequest) throws IOException {
        return client.nodes().info(nodesInfoRequest);
    }

    @Override
    protected NodesInfoRequest convertRequest(org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest nodesInfoRequest) throws IOException {
        NodesInfoRequest.Builder builder = new NodesInfoRequest.Builder();
        builder.metric(new ArrayList<>(nodesInfoRequest.requestedMetrics()));
        builder.nodeId(Arrays.asList(nodesInfoRequest.nodesIds()));
        Optional.ofNullable(nodesInfoRequest.timeout()).ifPresent(e -> builder.timeout(Time.of(t -> t.time(e.toString()))));
        return builder.build();
    }

    @Override
    protected NodesInfoResponse convertResponse(co.elastic.clients.elasticsearch.nodes.NodesInfoResponse nodesInfoResponse) throws IOException {
        Map<String, NodeInfo> nodeInfoMap = nodesInfoResponse.nodes();
        List<org.elasticsearch.action.admin.cluster.node.info.NodeInfo> nodes = new ArrayList<>();
        if (Objects.nonNull(nodeInfoMap)) {
            for (Map.Entry<String, co.elastic.clients.elasticsearch.nodes.info.NodeInfo> entry : nodeInfoMap.entrySet()) {
                co.elastic.clients.elasticsearch.nodes.info.NodeInfo nodeInfo = entry.getValue();
                Version version = Version.fromString(nodeInfo.version());
                Build.Type type = Build.Type.fromDisplayName(nodeInfo.buildType(), false);
                String hash = nodeInfo.buildHash();
                String date = "unknown";
                String minWireCompat = Version.CURRENT.minimumCompatibilityVersion().toString();
                String minIndexCompat = IndexVersion.getMinimumCompatibleIndexVersion(version.id()).toString();
                String displayString = Build.defaultDisplayString(type, hash, date, nodeInfo.version());
                Build build = new Build("default", type, hash, date, nodeInfo.version(), System.getProperty("build.version_qualifier"), false, minWireCompat, minIndexCompat, displayString);
                Set<DiscoveryNodeRole> roles = Optional.ofNullable(nodeInfo.roles()).orElse(Collections.emptyList()).stream().map(e -> DiscoveryNodeRole.maybeGetRoleFromRoleName(e.jsonValue()).orElse(null)).filter(Objects::nonNull).collect(Collectors.toSet());
                DiscoveryNode node = new DiscoveryNode(nodeInfo.name(), entry.getKey(),
                        parseAddress(nodeInfo.transportAddress()),
                        nodeInfo.attributes(), roles, VersionInformation.inferVersions(version));
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
                nodes.add(new org.elasticsearch.action.admin.cluster.node.info.NodeInfo(nodeInfo.version(), CompatibilityVersions.EMPTY, IndexVersion.current(), Collections.emptyMap(), build, node, settings, os, process, jvm, threadPool, transport, http, null, plugins, ingest, aggsInfo, totalIndexingBuffer));
            }
        }
        return new NodesInfoResponse(new ClusterName(nodesInfoResponse.clusterName()), nodes, Collections.emptyList());
    }
}
