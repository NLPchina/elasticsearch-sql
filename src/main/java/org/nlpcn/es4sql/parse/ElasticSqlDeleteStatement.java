package org.nlpcn.es4sql.parse;

import com.alibaba.druid.sql.ast.SQLCommentHint;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;

import java.util.ArrayList;
import java.util.List;

public class ElasticSqlDeleteStatement extends MySqlDeleteStatement {

    private List<SQLCommentHint> hints;

    public List<SQLCommentHint> getHints() {
        if (hints == null) {
            hints = new ArrayList<SQLCommentHint>(2);
        }

        return hints;
    }

}
