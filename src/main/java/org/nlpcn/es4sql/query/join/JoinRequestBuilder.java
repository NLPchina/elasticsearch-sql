package org.nlpcn.es4sql.query.join;

import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.nlpcn.es4sql.query.SqlElasticRequestBuilder;

import java.io.IOException;

/**
 * Created by Eliran on 15/9/2015.
 */
public  class JoinRequestBuilder implements SqlElasticRequestBuilder {

    private MultiSearchRequest multi;
    private TableInJoinRequestBuilder firstTable;
    private TableInJoinRequestBuilder secondTable;
    private SQLJoinTableSource.JoinType joinType;
    private int totalLimit;

    public JoinRequestBuilder() {
        firstTable = new TableInJoinRequestBuilder();
        secondTable = new TableInJoinRequestBuilder();
    }


    @Override
    public ActionRequest request() {
        if(multi == null)
            buildMulti();
        return multi;

    }

    private void buildMulti() {
        multi = new MultiSearchRequest();
        multi.add(firstTable.getRequestBuilder());
        multi.add(secondTable.getRequestBuilder());
    }

    @Override
    public String explain() {
        try {
            XContentBuilder firstBuilder = XContentFactory.contentBuilder(XContentType.JSON).prettyPrint();
            firstTable.getRequestBuilder().request().source().toXContent(firstBuilder, ToXContent.EMPTY_PARAMS);

            XContentBuilder secondBuilder = XContentFactory.contentBuilder(XContentType.JSON).prettyPrint();
            secondTable.getRequestBuilder().request().source().toXContent(secondBuilder, ToXContent.EMPTY_PARAMS);
            String explained = String.format(" first query:\n%s\n second query:\n%s", BytesReference.bytes(firstBuilder).utf8ToString(), BytesReference.bytes(secondBuilder).utf8ToString());

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

    @Override
    public ActionRequestBuilder getBuilder() {
        return this.firstTable.getRequestBuilder();
    }

    public MultiSearchRequest getMulti() {
        return multi;
    }

    public void setMulti(MultiSearchRequest multi) {
        this.multi = multi;
    }

    public SQLJoinTableSource.JoinType getJoinType() {
        return joinType;
    }

    public void setJoinType(SQLJoinTableSource.JoinType joinType) {
        this.joinType = joinType;
    }

    public TableInJoinRequestBuilder getFirstTable() {
        return firstTable;
    }

    public TableInJoinRequestBuilder getSecondTable() {
        return secondTable;
    }

    public int getTotalLimit() {
        return totalLimit;
    }

    public void setTotalLimit(int totalLimit) {
        this.totalLimit = totalLimit;
    }

}
