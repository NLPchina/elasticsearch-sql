package org.nlpcn.es4sql.query;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.nlpcn.es4sql.domain.Field;

import java.util.List;

/**
 * Created by Eliran on 28/8/2015.
 */
public class TableInJoinRequestBuilder {
    private SearchRequestBuilder requestBuilder;
    private String alias;
    private List<Field> returnedFields;

    public TableInJoinRequestBuilder() {
    }

    public SearchRequestBuilder getRequestBuilder() {
        return requestBuilder;
    }

    public void setRequestBuilder(SearchRequestBuilder requestBuilder) {
        this.requestBuilder = requestBuilder;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public List<Field> getReturnedFields() {
        return returnedFields;
    }

    public void setReturnedFields(List<Field> returnedFields) {
        this.returnedFields = returnedFields;
    }
}
