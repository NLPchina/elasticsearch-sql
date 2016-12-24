package org.nlpcn.es4sql.query;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;

/**
 * Created by Eliran on 19/8/2015.
 */
public interface SqlElasticRequestBuilder {
    ActionRequest request();
    String explain();
    ActionResponse get();
    ActionRequestBuilder getBuilder();

}
