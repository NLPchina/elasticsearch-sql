/*
 * Copyright 1999-2011 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.durid.sql.ast.statement;

import java.util.ArrayList;
import java.util.List;

import org.durid.sql.ast.SQLExpr;
import org.durid.sql.ast.SQLObjectImpl;
import org.durid.sql.ast.SQLStatement;
import org.durid.sql.visitor.SQLASTVisitor;

public class SQLInsertStatement extends SQLInsertInto implements SQLStatement {

    private static final long serialVersionUID = 1L;

    public SQLInsertStatement(){

    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            this.acceptChild(visitor, tableSource);
            this.acceptChild(visitor, columns);
            this.acceptChild(visitor, values);
            this.acceptChild(visitor, query);
        }

        visitor.endVisit(this);
    }

    public static class ValuesClause extends SQLObjectImpl {

        private static final long   serialVersionUID = 1L;
        private final List<SQLExpr> values           = new ArrayList<SQLExpr>();

        public List<SQLExpr> getValues() {
            return values;
        }

        public void output(StringBuffer buf) {
            buf.append(" VALUES (");
            for (int i = 0, size = values.size(); i < size; ++i) {
                if (i != 0) {
                    buf.append(", ");
                }
                values.get(i).output(buf);
            }
            buf.append(")");
        }

        @Override
        protected void accept0(SQLASTVisitor visitor) {
            if (visitor.visit(this)) {
                this.acceptChild(visitor, values);
            }

            visitor.endVisit(this);
        }
    }
}
