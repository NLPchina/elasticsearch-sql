package org.nlpcn.es4sql.jdbc;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by allwefantasy on 8/26/16.
 */
public interface ElasticsearchRel extends RelNode {
    void implement(Implementor implementor);

    /**
     * Calling convention for relational operations that occur in Elasticsearch.
     */
    Convention CONVENTION = new Convention.Impl("ELASTICSEARCH", ElasticsearchRel.class);

    /**
     * Callback for the implementation process that converts a tree of
     * {@link ElasticsearchRel} nodes into an Elasticsearch query.
     */
    class Implementor {
        final List<String> list = new ArrayList<>();

        RelOptTable table;
        ElasticsearchTable elasticsearchTable;

        public void add(String findOp) {
            list.add(findOp);
        }

        public void visitChild(int ordinal, RelNode input) {
            assert ordinal == 0;
            ((ElasticsearchRel) input).implement(this);
        }
    }
}

