package org.nlpcn.es4sql.jdbc;

/**
 * Created by allwefantasy on 8/26/16.
 */

import org.apache.calcite.adapter.elasticsearch.ElasticsearchToEnumerableConverterRule;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

/**
 * Relational expression representing a scan of an Elasticsearch type.
 *
 * <p> Additional operations might be applied,
 * using the "find" method.</p>
 */
public class ElasticsearchTableScan extends TableScan implements ElasticsearchRel {
    private final ElasticsearchTable elasticsearchTable;
    private final RelDataType projectRowType;

    /**
     * Creates an ElasticsearchTableScan.
     *
     * @param cluster Cluster
     * @param traitSet Trait set
     * @param table Table
     * @param elasticsearchTable Elasticsearch table
     * @param projectRowType Fields and types to project; null to project raw row
     */
    protected ElasticsearchTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table,
                                     ElasticsearchTable elasticsearchTable, RelDataType projectRowType) {
        super(cluster, traitSet, table);
        this.elasticsearchTable = elasticsearchTable;
        this.projectRowType = projectRowType;

        assert elasticsearchTable != null;
        assert getConvention() == ElasticsearchRel.CONVENTION;
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return this;
    }

    @Override public RelDataType deriveRowType() {
        return projectRowType != null ? projectRowType : super.deriveRowType();
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        final float f = projectRowType == null ? 1f : (float) projectRowType.getFieldCount() / 100f;
        return super.computeSelfCost(planner, mq).multiplyBy(.1 * f);
    }

    @Override public void register(RelOptPlanner planner) {
        planner.addRule(ElasticsearchToEnumerableConverterRule.INSTANCE);
//        for (RelOptRule rule: ElasticsearchRules.RULES) {
//            planner.addRule(rule);
//        }
    }

    @Override public void implement(Implementor implementor) {
        implementor.elasticsearchTable = elasticsearchTable;
        implementor.table = table;
    }
}
