package org.nlpcn.es4sql.domain;

import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;

import java.util.List;

/**
 * Created by Eliran on 20/8/2015.
 */
public class JoinSelect {
    private List<Condition> connectedConditions;

    //todo: make it an object
    private Select t1Select;
    private List<Field> t1ConnectedFields;
    private List<Field> t1SelectedFields;

    private Select t2Select;
    private List<Field> t2ConnectedFields;
    private List<Field> t2SelectedFields;
    private SQLJoinTableSource.JoinType joinType;

    public JoinSelect() {
    }

    public JoinSelect(List<Condition> connectedConditions, Select t1Select, List<Field> t1ConnectedFields, List<Field> t1SelectedFields, Select t2Select, List<Field> t2ConnectedFields, List<Field> t2SelectedFields,SQLJoinTableSource.JoinType joinType) {
        this.connectedConditions = connectedConditions;
        this.t1Select = t1Select;
        this.t1ConnectedFields = t1ConnectedFields;
        this.t1SelectedFields = t1SelectedFields;
        this.t2Select = t2Select;
        this.t2ConnectedFields = t2ConnectedFields;
        this.t2SelectedFields = t2SelectedFields;
        this.joinType = joinType;
    }

    public List<Condition> getConnectedConditions() {
        return connectedConditions;
    }

    public void setConnectedConditions(List<Condition> connectedConditions) {
        this.connectedConditions = connectedConditions;
    }

    public Select getT1Select() {
        return t1Select;
    }

    public void setT1Select(Select t1Select) {
        this.t1Select = t1Select;
    }

    public List<Field> getT1ConnectedFields() {
        return t1ConnectedFields;
    }

    public void setT1ConnectedFields(List<Field> t1ConnectedFields) {
        this.t1ConnectedFields = t1ConnectedFields;
    }

    public List<Field> getT1SelectedFields() {
        return t1SelectedFields;
    }

    public void setT1SelectedFields(List<Field> t1SelectedFields) {
        this.t1SelectedFields = t1SelectedFields;
    }

    public Select getT2Select() {
        return t2Select;
    }

    public void setT2Select(Select t2Select) {
        this.t2Select = t2Select;
    }

    public List<Field> getT2ConnectedFields() {
        return t2ConnectedFields;
    }

    public void setT2ConnectedFields(List<Field> t2ConnectedFields) {
        this.t2ConnectedFields = t2ConnectedFields;
    }

    public List<Field> getT2SelectedFields() {
        return t2SelectedFields;
    }

    public void setT2SelectedFields(List<Field> t2SelectedFields) {
        this.t2SelectedFields = t2SelectedFields;
    }

    public SQLJoinTableSource.JoinType getJoinType() {
        return joinType;
    }

    public void setJoinType(SQLJoinTableSource.JoinType joinType) {
        this.joinType = joinType;
    }
}
