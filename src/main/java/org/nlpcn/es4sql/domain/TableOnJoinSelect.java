package org.nlpcn.es4sql.domain;

import java.util.List;

/**
 * Created by Eliran on 28/8/2015.
 */
public class TableOnJoinSelect extends Select {

    private List<Field> connectedFields;
    private List<Field> selectedFields;
    private String alias;

    public TableOnJoinSelect() {
    }


    public List<Field> getConnectedFields() {
        return connectedFields;
    }

    public void setConnectedFields(List<Field> connectedFields) {
        this.connectedFields = connectedFields;
    }

    public List<Field> getSelectedFields() {
        return selectedFields;
    }

    public void setSelectedFields(List<Field> selectedFields) {
        this.selectedFields = selectedFields;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }
}
