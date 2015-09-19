package org.nlpcn.es4sql.query.join;

import org.elasticsearch.index.query.BoolFilterBuilder;
import org.nlpcn.es4sql.domain.Condition;
import org.nlpcn.es4sql.domain.Field;
import org.nlpcn.es4sql.domain.Where;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.maker.FilterMaker;
import org.nlpcn.es4sql.query.maker.Maker;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Eliran on 15/9/2015.
 */
public class NestedLoopsElasticRequestBuilder extends JoinRequestBuilder {
    private Map<Field,Condition> t1FieldToCondition;
    private int multiSearchMaxSize;
    public NestedLoopsElasticRequestBuilder() {
        t1FieldToCondition = new HashMap<>();
        multiSearchMaxSize = 100;
    }

    @Override
    public String explain() {
        String baseExplain = super.explain();
        Where where = Where.newInstance();
        for(Condition c : t1FieldToCondition.values()){
            where.addWhere(c);
        }
        BoolFilterBuilder explan = null;
        try {
            explan = FilterMaker.explan(where);
        } catch (SqlParseException e) {
        }
        String conditions = explan == null ? "Could not parse conditions" : explan.toString();
        String nestedExplain =  "Nested Loops \n run first query , and for each result run second query with additional conditions :\n" +conditions +"\n"+  baseExplain;
        return nestedExplain;
    }

    public Map<Field, Condition> getT1FieldToCondition() {
        return t1FieldToCondition;
    }

    public void setT1FieldToCondition(Map<Field, Condition> t1FieldToCondition) {
        this.t1FieldToCondition = t1FieldToCondition;
    }

    public int getMultiSearchMaxSize() {
        return multiSearchMaxSize;
    }

    public void setMultiSearchMaxSize(int multiSearchMaxSize) {
        this.multiSearchMaxSize = multiSearchMaxSize;
    }

    public void addConditionMapping(Condition c){
        t1FieldToCondition.put(new Field(c.getName(),null),c);
    }
}
