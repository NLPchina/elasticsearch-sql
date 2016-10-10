package org.nlpcn.es4sql.query.multi;

import com.alibaba.druid.sql.ast.statement.SQLUnionOperator;
import org.nlpcn.es4sql.domain.Select;

/**
 * Created by Eliran on 19/8/2016.
 */
public class MultiQuerySelect {
    private SQLUnionOperator operation;
    private Select firstSelect;
    private Select secondSelect;

    public MultiQuerySelect(SQLUnionOperator operation, Select firstSelect, Select secondSelect) {
        this.operation = operation;
        this.firstSelect = firstSelect;
        this.secondSelect = secondSelect;
    }

    public SQLUnionOperator getOperation() {
        return operation;
    }

    public Select getFirstSelect() {
        return firstSelect;
    }

    public Select getSecondSelect() {
        return secondSelect;
    }
}
