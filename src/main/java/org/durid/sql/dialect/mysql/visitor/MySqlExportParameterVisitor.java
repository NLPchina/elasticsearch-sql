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
package org.durid.sql.dialect.mysql.visitor;

import java.util.ArrayList;
import java.util.List;

import org.durid.sql.ast.SQLOrderBy;
import org.durid.sql.ast.expr.SQLBetweenExpr;
import org.durid.sql.ast.expr.SQLBinaryOpExpr;
import org.durid.sql.ast.expr.SQLInListExpr;
import org.durid.sql.ast.expr.SQLMethodInvokeExpr;
import org.durid.sql.ast.statement.SQLSelectGroupByClause;
import org.durid.sql.ast.statement.SQLSelectItem;
import org.durid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock.Limit;
import org.durid.sql.visitor.ExportParameterVisitor;
import org.durid.sql.visitor.ExportParameterVisitorUtils;

public class MySqlExportParameterVisitor extends MySqlASTVisitorAdapter implements ExportParameterVisitor {

    private final List<Object> parameters;
    
    public MySqlExportParameterVisitor() {
        this(new ArrayList<Object>());
    }

    public MySqlExportParameterVisitor(List<Object> parameters){
        this.parameters = parameters;
    }

    public List<Object> getParameters() {
        return parameters;
    }

    @Override
    public boolean visit(SQLSelectItem x) {
        return false;
    }

    @Override
    public boolean visit(Limit x) {
        return false;
    }

    @Override
    public boolean visit(SQLOrderBy x) {
        return false;
    }

    @Override
    public boolean visit(SQLSelectGroupByClause x) {
        return false;
    }

    @Override
    public boolean visit(SQLMethodInvokeExpr x) {
        ExportParameterVisitorUtils.exportParamterAndAccept(this.parameters, x.getParameters());

        return true;
    }

    @Override
    public boolean visit(SQLInListExpr x) {
        ExportParameterVisitorUtils.exportParamterAndAccept(this.parameters, x.getTargetList());

        return true;
    }

    @Override
    public boolean visit(SQLBetweenExpr x) {
        ExportParameterVisitorUtils.exportParameter(this.parameters, x);
        return true;
    }

    public boolean visit(SQLBinaryOpExpr x) {
        ExportParameterVisitorUtils.exportParameter(this.parameters, x);
        return true;
    }

}
