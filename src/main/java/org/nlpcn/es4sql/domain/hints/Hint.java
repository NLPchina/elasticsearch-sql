package org.nlpcn.es4sql.domain.hints;

/**
 * Created by Eliran on 5/9/2015.
 */
public class Hint {
    private HintType type;
    private Object[] params;
    public Hint(HintType type,Object[] params) {
        this.type = type;
        this.params = params;
    }

    public HintType getType() {
        return type;
    }

    public Object[] getParams() {
        return params;
    }
}
