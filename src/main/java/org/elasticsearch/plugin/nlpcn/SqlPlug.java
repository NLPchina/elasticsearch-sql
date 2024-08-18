package org.elasticsearch.plugin.nlpcn;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class SqlPlug extends Plugin implements ActionPlugin {

    public SqlPlug() {
    }

    public String name() {
        return "sql";
    }

    public String description() {
        return "Use sql to query elasticsearch.";
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        return Collections.singletonList(new NamedXContentRegistryHolder(services.xContentRegistry()));
    }

    @Override
    public Collection<RestHandler> getRestHandlers(Settings settings, NamedWriteableRegistry namedWriteableRegistry, RestController restController, ClusterSettings clusterSettings, IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver, Supplier<DiscoveryNodes> nodesInCluster, Predicate<NodeFeature> clusterSupportsFeature) {
        return Collections.singletonList(new RestSqlAction());
    }
}
