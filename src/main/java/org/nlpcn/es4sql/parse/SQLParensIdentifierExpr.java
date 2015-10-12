package org.nlpcn.es4sql.parse;

import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;


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

/**
 * An Identifier that is wrapped in parenthesis.
 * This is for tracking in group bys the difference between "group by state, age" and "group by (state), (age)".
 * For non group by identifiers, it acts as a normal SQLIdentifierExpr.
 */
public class SQLParensIdentifierExpr extends SQLIdentifierExpr {

    public SQLParensIdentifierExpr() {
    }

    public SQLParensIdentifierExpr(String name) {
        super(name);
    }

    public SQLParensIdentifierExpr(SQLIdentifierExpr expr) {
        super(expr.getName());
    }
}
