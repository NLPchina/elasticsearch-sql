package org.nlpcn.es4sql.query.join;

import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.nlpcn.es4sql.domain.Field;
import org.nlpcn.es4sql.query.SqlElasticRequestBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by Eliran on 22/8/2015.
 */
public class HashJoinElasticRequestBuilder  extends JoinRequestBuilder {

    private List<List<Map.Entry<Field,Field>>> t1ToT2FieldsComparison;
    private boolean useTermFiltersOptimization;

    public HashJoinElasticRequestBuilder() {
    }

    @Override
    public String explain() {
      return "HashJoin "+ super.explain();
    }

    public List<List<Map.Entry<Field,Field>>> getT1ToT2FieldsComparison() {
        return t1ToT2FieldsComparison;
    }

    public void setT1ToT2FieldsComparison(List<List<Map.Entry<Field,Field>>> t1ToT2FieldsComparison) {
        this.t1ToT2FieldsComparison = t1ToT2FieldsComparison;
    }

    public boolean isUseTermFiltersOptimization() {
        return useTermFiltersOptimization;
    }

    public void setUseTermFiltersOptimization(boolean useTermFiltersOptimization) {
        this.useTermFiltersOptimization = useTermFiltersOptimization;
    }
}
