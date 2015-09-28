package org.nlpcn.es4sql.parse;

import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;


/**
 * Created by jheimbouch on 3/17/15.
 */
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



public class SQLOdbcExpr extends SQLCharExpr {

    private static final long serialVersionUID = 1L;

    public SQLOdbcExpr(){

    }

    public SQLOdbcExpr(String text){
        super(text);
    }

    @Override
    public void output(StringBuffer buf) {
        if ((this.text == null) || (this.text.length() == 0)) {
            buf.append("NULL");
        } else {
            buf.append("{ts '");
            buf.append(this.text.replaceAll("'", "''"));
            buf.append("'}");
        }
    }

    @Override
    public String getText() {
        StringBuilder sb = new StringBuilder();
        sb.append("{ts '");
        sb.append(this.text);
        sb.append("'}");
        return sb.toString();
    }

    protected void accept0(SQLASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }
}

