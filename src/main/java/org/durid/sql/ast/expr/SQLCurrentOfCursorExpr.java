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
package org.durid.sql.ast.expr;

import org.durid.sql.ast.SQLExprImpl;
import org.durid.sql.ast.SQLName;
import org.durid.sql.visitor.SQLASTVisitor;

public class SQLCurrentOfCursorExpr extends SQLExprImpl {

    private static final long serialVersionUID = 1L;

    private SQLName           cursorName;

    public SQLCurrentOfCursorExpr(){

    }

    public SQLCurrentOfCursorExpr(SQLName cursorName){
        this.cursorName = cursorName;
    }

    public SQLName getCursorName() {
        return cursorName;
    }

    public void setCursorName(SQLName cursorName) {
        this.cursorName = cursorName;
    }

    @Override
    public void output(StringBuffer buf) {
        buf.append("CURRENT OF ");
        cursorName.output(buf);
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, this.cursorName);
        }
        visitor.endVisit(this);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((cursorName == null) ? 0 : cursorName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        SQLCurrentOfCursorExpr other = (SQLCurrentOfCursorExpr) obj;
        if (cursorName == null) {
            if (other.cursorName != null) {
                return false;
            }
        } else if (!cursorName.equals(other.cursorName)) {
            return false;
        }
        return true;
    }

}
