package org.nlpcn.es4sql.query;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;

/**
 * Created by Eliran on 19/8/2015.
 */
public class SqlElasticSearchRequestBuilder implements SqlElasticRequestBuilder {
    ActionRequestBuilder requestBuilder;

    public SqlElasticSearchRequestBuilder(ActionRequestBuilder requestBuilder) {
        this.requestBuilder = requestBuilder;
    }

    @Override
    public ActionRequest request() {
        return requestBuilder.request();
    }

    @Override
    public String explain() {
        return requestBuilder.toString();
    }

    @Override
    public ActionResponse get() {
        return requestBuilder.get();
    }

    @Override
    public ActionRequestBuilder getBuilder() {
        return requestBuilder;
    }

    @Override
    public String toString() {
        return this.requestBuilder.toString();
    }
}
