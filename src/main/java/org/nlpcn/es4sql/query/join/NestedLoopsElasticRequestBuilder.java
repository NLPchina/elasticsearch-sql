package org.nlpcn.es4sql.query.join;

import org.nlpcn.es4sql.domain.Condition;
import org.nlpcn.es4sql.domain.Field;

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
