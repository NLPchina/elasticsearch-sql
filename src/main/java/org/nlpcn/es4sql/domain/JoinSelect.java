package org.nlpcn.es4sql.domain;

import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;

import java.util.List;

/**
 * Created by Eliran on 20/8/2015.
 */
public class JoinSelect {


    private TableOnJoinSelect firstTable;
    private TableOnJoinSelect secondTable;
    private List<Condition> connectedConditions;
    private List<Hint> hints;

    private SQLJoinTableSource.JoinType joinType;


    public JoinSelect() {
        firstTable = new TableOnJoinSelect();
        secondTable = new TableOnJoinSelect();
    }


    public List<Condition> getConnectedConditions() {
        return connectedConditions;
    }

    public void setConnectedConditions(List<Condition> connectedConditions) {
        this.connectedConditions = connectedConditions;
    }

    public TableOnJoinSelect getFirstTable() {
        return firstTable;
    }

    public TableOnJoinSelect getSecondTable() {
        return secondTable;
    }



    public SQLJoinTableSource.JoinType getJoinType() {
        return joinType;
    }

    public void setJoinType(SQLJoinTableSource.JoinType joinType) {
        this.joinType = joinType;
    }

    public List<Hint> getHints() {
        return hints;
    }

    public void setHints(List<Hint> hints) {
        this.hints = hints;
    }
}
