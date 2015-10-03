package org.nlpcn.es4sql.parse;

import org.nlpcn.es4sql.domain.Select;

/**
 * Created by Eliran on 3/10/2015.
 */
public class SubQueryExpression {
    private Object[] values;
    private Select select;
    private String returnField;

    public SubQueryExpression(Select innerSelect) {
        this.select = innerSelect;
        this.returnField = select.getFields().get(0).getName();
        values = null;
    }

    public Object[] getValues() {
        return values;
    }

    public void setValues(Object[] values) {
        this.values = values;
    }

    public Select getSelect() {
        return select;
    }

    public void setSelect(Select select) {
        this.select = select;
    }

    public String getReturnField() {
        return returnField;
    }
}
