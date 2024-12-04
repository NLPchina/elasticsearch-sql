package org.elasticsearch.plugin.nlpcn;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;

import java.util.Collections;
import java.util.List;
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
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings, IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver, Supplier<DiscoveryNodes> nodesInCluster) {
        return Collections.singletonList(new RestSqlAction());
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return Collections.singletonList(new FixedExecutorBuilder(settings, "nlpcn_sql", 10, 100, "thread_pool.nlpcn_sql"));
    }
}
