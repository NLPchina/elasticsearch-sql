package org.nlpcn.es4sql.query;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.support.QuerySourceBuilder;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Created by Eliran on 19/8/2015.
 */
public class SqlElasticDeleteByQueryRequestBuilder implements SqlElasticRequestBuilder {
    DeleteByQueryRequestBuilder deleteByQueryRequestBuilder;

    public SqlElasticDeleteByQueryRequestBuilder(DeleteByQueryRequestBuilder deleteByQueryRequestBuilder) {
        this.deleteByQueryRequestBuilder = deleteByQueryRequestBuilder;
    }

    @Override
    public ActionRequest request() {
        return deleteByQueryRequestBuilder.request();
    }

    @Override
    public String explain() {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON).prettyPrint();
            Method method = deleteByQueryRequestBuilder.getClass().getDeclaredMethod("sourceBuilder");
            method.setAccessible(true);
            QuerySourceBuilder sourceBuilder = (QuerySourceBuilder) method.invoke(deleteByQueryRequestBuilder);
            sourceBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
            return builder.string();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public ActionResponse get() {

        return this.deleteByQueryRequestBuilder.get();
    }

    @Override
    public ActionRequestBuilder getBuilder() {
        return deleteByQueryRequestBuilder;
    }

}
