package org.nlpcn.es4sql.query;

import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.nlpcn.es4sql.domain.Field;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by Eliran on 22/8/2015.
 */
public class HashJoinElasticRequestBuilder  implements SqlElasticRequestBuilder{

    private SearchRequestBuilder firstTableRequest;
    private SearchRequestBuilder secondTableRequest;
    private MultiSearchRequest multi;
    private List<Field> firstTableReturnedField;
    private List<Field> secondTableReturnedField;
    private List<Map.Entry<Field,Field>> t1ToT2FieldsComparison;
    private SQLJoinTableSource.JoinType joinType;

    public HashJoinElasticRequestBuilder() {
    }


    @Override
    public ActionRequest request() {
        if(multi == null)
            buildMulti();
        return multi;

    }

    private void buildMulti() {
        multi = new MultiSearchRequest();
        multi.add(firstTableRequest);
        multi.add(secondTableRequest);
        multi.listenerThreaded(false);
    }

    @Override
    public String explain() {
        try {
            XContentBuilder firstBuilder = XContentFactory.contentBuilder(XContentType.JSON).prettyPrint();
            firstTableRequest.internalBuilder().toXContent(firstBuilder, ToXContent.EMPTY_PARAMS);

            XContentBuilder secondBuilder = XContentFactory.contentBuilder(XContentType.JSON).prettyPrint();
            secondTableRequest.internalBuilder().toXContent(secondBuilder, ToXContent.EMPTY_PARAMS);
            String explained = String.format("HashJoin. first query:\n%s\n second query:\n%s", firstBuilder.string(), secondBuilder.string());

            return explained;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public ActionResponse get() {
        return null;
    }


    public SearchRequestBuilder getFirstTableRequest() {
        return firstTableRequest;
    }

    public void setFirstTableRequest(SearchRequestBuilder firstTableRequest) {
        this.firstTableRequest = firstTableRequest;
    }

    public SearchRequestBuilder getSecondTableRequest() {
        return secondTableRequest;
    }

    public void setSecondTableRequest(SearchRequestBuilder secondTableRequest) {
        this.secondTableRequest = secondTableRequest;
    }

    public MultiSearchRequest getMulti() {
        return multi;
    }

    public void setMulti(MultiSearchRequest multi) {
        this.multi = multi;
    }

    public List<Field> getFirstTableReturnedField() {
        return firstTableReturnedField;
    }

    public void setFirstTableReturnedField(List<Field> firstTableReturnedField) {
        this.firstTableReturnedField = firstTableReturnedField;
    }

    public List<Field> getSecondTableReturnedField() {
        return secondTableReturnedField;
    }

    public void setSecondTableReturnedField(List<Field> secondTableReturnedField) {
        this.secondTableReturnedField = secondTableReturnedField;
    }

    public List<Map.Entry<Field, Field>> getT1ToT2FieldsComparison() {
        return t1ToT2FieldsComparison;
    }

    public void setT1ToT2FieldsComparison(List<Map.Entry<Field, Field>> t1ToT2FieldsComparison) {
        this.t1ToT2FieldsComparison = t1ToT2FieldsComparison;
    }

    public SQLJoinTableSource.JoinType getJoinType() {
        return joinType;
    }

    public void setJoinType(SQLJoinTableSource.JoinType joinType) {
        this.joinType = joinType;
    }

}
