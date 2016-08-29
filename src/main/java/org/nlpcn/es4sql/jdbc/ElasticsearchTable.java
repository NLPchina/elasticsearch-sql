package org.nlpcn.es4sql.jdbc;



import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.*;
import org.apache.calcite.linq4j.function.Function1;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.*;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeName;

import org.apache.calcite.util.Util;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.search.SearchHit;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Table based on an Elasticsearch type.
 */
public class ElasticsearchTable extends AbstractQueryableTable implements TranslatableTable {
    private final Client client;
    private final String indexName;
    private final String typeName;
    private final MappingMetaData mappingMetaData;

    /**
     * Creates an ElasticsearchTable.
     */
    public ElasticsearchTable(Client client, String indexName,
                              String typeName, MappingMetaData mappingMetaData) {
        super(Object[].class);
        this.client = client;
        this.indexName = indexName;
        this.typeName = typeName;
        this.mappingMetaData = mappingMetaData;
    }

    @Override public String toString() {
        return "ElasticsearchTable{" + typeName + "}";
    }

    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
        final RelDataType mapType = relDataTypeFactory.createMapType(
                relDataTypeFactory.createSqlType(SqlTypeName.VARCHAR),
                relDataTypeFactory.createTypeWithNullability(
                        relDataTypeFactory.createSqlType(SqlTypeName.ANY),
                        true));
        return relDataTypeFactory.builder().add("_MAP", mapType).build();
    }

    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema,
                                        String tableName) {
        return new ElasticsearchQueryable<>(queryProvider, schema, this, tableName);
    }

    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        final RelOptCluster cluster = context.getCluster();
        return new ElasticsearchTableScan(cluster, cluster.traitSetOf(ElasticsearchRel.CONVENTION),
                relOptTable, this, null);
    }

    /** Executes a "find" operation on the underlying type.
     *
     * <p>For example,
     * <code>client.prepareSearch(index).setTypes(type)
     * .setSource("{\"fields\" : [\"state\"]}")</code></p>
     *
     * @param index Elasticsearch index
     * @param ops List of operations represented as Json strings.
     * @param fields List of fields to project; or null to return map
     * @return Enumerator of results
     */
    private Enumerable<Object> find(String index, List<String> ops,
                                    List<Map.Entry<String, Class>> fields) {
        final String dbName = index;

        final String queryString = "{" + Util.toString(ops, "", ", ", "") + "}";

        final Function1<SearchHit, Object> getter = ElasticsearchEnumerator.getter(fields);

        return new AbstractEnumerable<Object>() {
            public Enumerator<Object> enumerator() {
                final Iterator<SearchHit> cursor = client.prepareSearch(dbName).setTypes(typeName)
                        .setSource(queryString).execute().actionGet().getHits().iterator();
                return new ElasticsearchEnumerator(cursor, getter);
            }
        };
    }

    /**
     * Implementation of {@link org.apache.calcite.linq4j.Queryable} based on
     * a {@link org.apache.calcite.adapter.elasticsearch.ElasticsearchTable}.
     */
    public static class ElasticsearchQueryable<T> extends AbstractTableQueryable<T> {
        public ElasticsearchQueryable(QueryProvider queryProvider, SchemaPlus schema,
                                      ElasticsearchTable table, String tableName) {
            super(queryProvider, schema, table, tableName);
        }

        public Enumerator<T> enumerator() {
            return null;
        }

        private String getIndex() {
            return schema.unwrap(ElasticsearchSchema.class).index;
        }

        private ElasticsearchTable getTable() {
            return (ElasticsearchTable) table;
        }

        /** Called via code-generation.
         *
         * @see org.apache.calcite.adapter.elasticsearch.ElasticsearchMethod#ELASTICSEARCH_QUERYABLE_FIND
         */
        @SuppressWarnings("UnusedDeclaration")
        public Enumerable<Object> find(List<String> ops,
                                       List<Map.Entry<String, Class>> fields) {
            return getTable().find(getIndex(), ops, fields);
        }
    }


}
