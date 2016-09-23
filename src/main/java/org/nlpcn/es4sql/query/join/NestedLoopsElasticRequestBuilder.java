package org.nlpcn.es4sql.query.join;


import org.elasticsearch.index.query.QueryBuilder;
import org.nlpcn.es4sql.domain.Condition;
import org.nlpcn.es4sql.domain.Field;
import org.nlpcn.es4sql.domain.Where;
import org.nlpcn.es4sql.exception.SqlParseException;

import org.nlpcn.es4sql.query.maker.Maker;
import org.nlpcn.es4sql.query.maker.QueryMaker;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Eliran on 15/9/2015.
 */
public class NestedLoopsElasticRequestBuilder extends JoinRequestBuilder {

    private Where connectedWhere;
    private int multiSearchMaxSize;
    public NestedLoopsElasticRequestBuilder() {

        multiSearchMaxSize = 100;
    }

    @Override
    public String explain() {
        String baseExplain = super.explain();
        Where where = this.connectedWhere;
        QueryBuilder explan = null;
        try {
            if(where!=null)
                explan = QueryMaker.explan(where,false);
        } catch (SqlParseException e) {
        }
        String conditions = explan == null ? "Could not parse conditions" : explan.toString();
        String nestedExplain =  "Nested Loops \n run first query , and for each result run second query with additional conditions :\n" +conditions +"\n"+  baseExplain;
        return nestedExplain;
    }


    public int getMultiSearchMaxSize() {
        return multiSearchMaxSize;
    }

    public void setMultiSearchMaxSize(int multiSearchMaxSize) {
        this.multiSearchMaxSize = multiSearchMaxSize;
    }

    public Where getConnectedWhere() {
        return connectedWhere;
    }

    public void setConnectedWhere(Where connectedWhere) {
        this.connectedWhere = connectedWhere;
    }
}
