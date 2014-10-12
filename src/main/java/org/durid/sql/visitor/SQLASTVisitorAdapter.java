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
package org.durid.sql.visitor;

import org.durid.sql.ast.SQLCommentHint;
import org.durid.sql.ast.SQLDataType;
import org.durid.sql.ast.SQLObject;
import org.durid.sql.ast.SQLOrderBy;
import org.durid.sql.ast.SQLOver;
import org.durid.sql.ast.expr.SQLAggregateExpr;
import org.durid.sql.ast.expr.SQLAllColumnExpr;
import org.durid.sql.ast.expr.SQLAllExpr;
import org.durid.sql.ast.expr.SQLAnyExpr;
import org.durid.sql.ast.expr.SQLBetweenExpr;
import org.durid.sql.ast.expr.SQLBinaryOpExpr;
import org.durid.sql.ast.expr.SQLBitStringLiteralExpr;
import org.durid.sql.ast.expr.SQLCaseExpr;
import org.durid.sql.ast.expr.SQLCastExpr;
import org.durid.sql.ast.expr.SQLCharExpr;
import org.durid.sql.ast.expr.SQLCurrentOfCursorExpr;
import org.durid.sql.ast.expr.SQLDateLiteralExpr;
import org.durid.sql.ast.expr.SQLDefaultExpr;
import org.durid.sql.ast.expr.SQLExistsExpr;
import org.durid.sql.ast.expr.SQLHexExpr;
import org.durid.sql.ast.expr.SQLHexStringLiteralExpr;
import org.durid.sql.ast.expr.SQLIdentifierExpr;
import org.durid.sql.ast.expr.SQLInListExpr;
import org.durid.sql.ast.expr.SQLInSubQueryExpr;
import org.durid.sql.ast.expr.SQLIntegerExpr;
import org.durid.sql.ast.expr.SQLIntervalLiteralExpr;
import org.durid.sql.ast.expr.SQLListExpr;
import org.durid.sql.ast.expr.SQLMethodInvokeExpr;
import org.durid.sql.ast.expr.SQLNCharExpr;
import org.durid.sql.ast.expr.SQLNotExpr;
import org.durid.sql.ast.expr.SQLNullExpr;
import org.durid.sql.ast.expr.SQLNumberExpr;
import org.durid.sql.ast.expr.SQLPropertyExpr;
import org.durid.sql.ast.expr.SQLQueryExpr;
import org.durid.sql.ast.expr.SQLSomeExpr;
import org.durid.sql.ast.expr.SQLUnaryExpr;
import org.durid.sql.ast.expr.SQLVariantRefExpr;
import org.durid.sql.ast.statement.NotNullConstraint;
import org.durid.sql.ast.statement.SQLAlterTableAddColumn;
import org.durid.sql.ast.statement.SQLAlterTableAddPrimaryKey;
import org.durid.sql.ast.statement.SQLAlterTableDropColumnItem;
import org.durid.sql.ast.statement.SQLAlterTableDropIndex;
import org.durid.sql.ast.statement.SQLAssignItem;
import org.durid.sql.ast.statement.SQLCallStatement;
import org.durid.sql.ast.statement.SQLColumnDefinition;
import org.durid.sql.ast.statement.SQLCommentStatement;
import org.durid.sql.ast.statement.SQLCreateDatabaseStatement;
import org.durid.sql.ast.statement.SQLCreateTableStatement;
import org.durid.sql.ast.statement.SQLCreateViewStatement;
import org.durid.sql.ast.statement.SQLDeleteStatement;
import org.durid.sql.ast.statement.SQLDropIndexStatement;
import org.durid.sql.ast.statement.SQLDropTableStatement;
import org.durid.sql.ast.statement.SQLDropViewStatement;
import org.durid.sql.ast.statement.SQLExprTableSource;
import org.durid.sql.ast.statement.SQLInsertStatement;
import org.durid.sql.ast.statement.SQLInsertStatement.ValuesClause;
import org.durid.sql.ast.statement.SQLJoinTableSource;
import org.durid.sql.ast.statement.SQLReleaseSavePointStatement;
import org.durid.sql.ast.statement.SQLRollbackStatement;
import org.durid.sql.ast.statement.SQLSavePointStatement;
import org.durid.sql.ast.statement.SQLSelect;
import org.durid.sql.ast.statement.SQLSelectGroupByClause;
import org.durid.sql.ast.statement.SQLSelectItem;
import org.durid.sql.ast.statement.SQLSelectOrderByItem;
import org.durid.sql.ast.statement.SQLSelectQueryBlock;
import org.durid.sql.ast.statement.SQLSelectStatement;
import org.durid.sql.ast.statement.SQLSetStatement;
import org.durid.sql.ast.statement.SQLSubqueryTableSource;
import org.durid.sql.ast.statement.SQLTableElement;
import org.durid.sql.ast.statement.SQLTruncateStatement;
import org.durid.sql.ast.statement.SQLUnionQuery;
import org.durid.sql.ast.statement.SQLUniqueConstraint;
import org.durid.sql.ast.statement.SQLUpdateSetItem;
import org.durid.sql.ast.statement.SQLUpdateStatement;
import org.durid.sql.ast.statement.SQLUseStatement;

public class SQLASTVisitorAdapter implements SQLASTVisitor {

    public void endVisit(SQLAllColumnExpr x) {
    }

    public void endVisit(SQLBetweenExpr x) {
    }

    public void endVisit(SQLBinaryOpExpr x) {
    }

    public void endVisit(SQLCaseExpr x) {
    }

    public void endVisit(SQLCaseExpr.Item x) {
    }

    public void endVisit(SQLCharExpr x) {
    }

    public void endVisit(SQLIdentifierExpr x) {
    }

    public void endVisit(SQLInListExpr x) {
    }

    public void endVisit(SQLIntegerExpr x) {
    }

    public void endVisit(SQLExistsExpr x) {
    }

    public void endVisit(SQLNCharExpr x) {
    }

    public void endVisit(SQLNotExpr x) {
    }

    public void endVisit(SQLNullExpr x) {
    }

    public void endVisit(SQLNumberExpr x) {
    }

    public void endVisit(SQLPropertyExpr x) {
    }

    public void endVisit(SQLSelectGroupByClause x) {
    }

    public void endVisit(SQLSelectItem x) {
    }

    public void endVisit(SQLSelectStatement selectStatement) {
    }

    public void postVisit(SQLObject astNode) {
    }

    public void preVisit(SQLObject astNode) {
    }

    public boolean visit(SQLAllColumnExpr x) {
        return true;
    }

    public boolean visit(SQLBetweenExpr x) {
        return true;
    }

    public boolean visit(SQLBinaryOpExpr x) {
        return true;
    }

    public boolean visit(SQLCaseExpr x) {
        return true;
    }

    public boolean visit(SQLCaseExpr.Item x) {
        return true;
    }

    public boolean visit(SQLCastExpr x) {
        return true;
    }

    public boolean visit(SQLCharExpr x) {
        return true;
    }

    public boolean visit(SQLExistsExpr x) {
        return true;
    }

    public boolean visit(SQLIdentifierExpr x) {
        return true;
    }

    public boolean visit(SQLInListExpr x) {
        return true;
    }

    public boolean visit(SQLIntegerExpr x) {
        return true;
    }

    public boolean visit(SQLNCharExpr x) {
        return true;
    }

    public boolean visit(SQLNotExpr x) {
        return true;
    }

    public boolean visit(SQLNullExpr x) {
        return true;
    }

    public boolean visit(SQLNumberExpr x) {
        return true;
    }

    public boolean visit(SQLPropertyExpr x) {
        return true;
    }

    public boolean visit(SQLSelectGroupByClause x) {
        return true;
    }

    public boolean visit(SQLSelectItem x) {
        return true;
    }

    public void endVisit(SQLCastExpr x) {
    }

    public boolean visit(SQLSelectStatement astNode) {
        return true;
    }

    public void endVisit(SQLAggregateExpr x) {
    }

    public boolean visit(SQLAggregateExpr x) {
        return true;
    }

    public boolean visit(SQLVariantRefExpr x) {
        return true;
    }

    public void endVisit(SQLVariantRefExpr x) {
    }

    public boolean visit(SQLQueryExpr x) {
        return true;
    }

    public void endVisit(SQLQueryExpr x) {
    }

    public boolean visit(SQLBitStringLiteralExpr x) {
        return true;
    }

    public void endVisit(SQLBitStringLiteralExpr x) {
    }

    public boolean visit(SQLHexStringLiteralExpr x) {
        return true;
    }

    public void endVisit(SQLHexStringLiteralExpr x) {
    }

    public boolean visit(SQLDateLiteralExpr x) {
        return true;
    }

    public void endVisit(SQLDateLiteralExpr x) {
    }

    public boolean visit(SQLSelect x) {
        return true;
    }

    public void endVisit(SQLSelect select) {
    }

    public boolean visit(SQLSelectQueryBlock x) {
        return true;
    }

    public void endVisit(SQLSelectQueryBlock x) {
    }

    public boolean visit(SQLExprTableSource x) {
        return true;
    }

    public void endVisit(SQLExprTableSource x) {
    }

    public boolean visit(SQLIntervalLiteralExpr x) {
        return true;
    }

    public void endVisit(SQLIntervalLiteralExpr x) {
    }

    public boolean visit(SQLOrderBy x) {
        return true;
    }

    public void endVisit(SQLOrderBy x) {
    }

    public boolean visit(SQLSelectOrderByItem x) {
        return true;
    }

    public void endVisit(SQLSelectOrderByItem x) {
    }

    public boolean visit(SQLDropTableStatement x) {
        return true;
    }

    public void endVisit(SQLDropTableStatement x) {
    }

    public boolean visit(SQLCreateTableStatement x) {
        return true;
    }

    public void endVisit(SQLCreateTableStatement x) {
    }

    public boolean visit(SQLTableElement x) {
        return true;
    }

    public void endVisit(SQLTableElement x) {
    }

    public boolean visit(SQLColumnDefinition x) {
        return true;
    }

    public void endVisit(SQLColumnDefinition x) {
    }

    public boolean visit(SQLDataType x) {
        return true;
    }

    public void endVisit(SQLDataType x) {
    }

    public boolean visit(SQLDeleteStatement x) {
        return true;
    }

    public void endVisit(SQLDeleteStatement x) {
    }

    public boolean visit(SQLCurrentOfCursorExpr x) {
        return true;
    }

    public void endVisit(SQLCurrentOfCursorExpr x) {
    }

    public boolean visit(SQLInsertStatement x) {
        return true;
    }

    public void endVisit(SQLInsertStatement x) {
    }

    public boolean visit(SQLUpdateSetItem x) {
        return true;
    }

    public void endVisit(SQLUpdateSetItem x) {
    }

    public boolean visit(SQLUpdateStatement x) {
        return true;
    }

    public void endVisit(SQLUpdateStatement x) {
    }

    public boolean visit(SQLCreateViewStatement x) {
        return true;
    }

    public void endVisit(SQLCreateViewStatement x) {
    }

    public boolean visit(SQLUniqueConstraint x) {
        return true;
    }

    public void endVisit(SQLUniqueConstraint x) {
    }

    public boolean visit(NotNullConstraint x) {
        return true;
    }

    public void endVisit(NotNullConstraint x) {
    }

    @Override
    public void endVisit(SQLMethodInvokeExpr x) {

    }

    @Override
    public boolean visit(SQLMethodInvokeExpr x) {
        return true;
    }

    @Override
    public void endVisit(SQLUnionQuery x) {

    }

    @Override
    public boolean visit(SQLUnionQuery x) {
        return true;
    }

    @Override
    public boolean visit(SQLUnaryExpr x) {
        return true;
    }

    @Override
    public void endVisit(SQLUnaryExpr x) {

    }

    @Override
    public boolean visit(SQLHexExpr x) {
        return false;
    }

    @Override
    public void endVisit(SQLHexExpr x) {

    }

    @Override
    public void endVisit(SQLSetStatement x) {

    }

    @Override
    public boolean visit(SQLSetStatement x) {
        return true;
    }

    @Override
    public void endVisit(SQLAssignItem x) {

    }

    @Override
    public boolean visit(SQLAssignItem x) {
        return true;
    }

    @Override
    public void endVisit(SQLCallStatement x) {

    }

    @Override
    public boolean visit(SQLCallStatement x) {
        return true;
    }

    @Override
    public void endVisit(SQLJoinTableSource x) {

    }

    @Override
    public boolean visit(SQLJoinTableSource x) {
        return true;
    }

    @Override
    public boolean visit(ValuesClause x) {
        return true;
    }

    @Override
    public void endVisit(ValuesClause x) {

    }

    @Override
    public void endVisit(SQLSomeExpr x) {

    }

    @Override
    public boolean visit(SQLSomeExpr x) {
        return true;
    }

    @Override
    public void endVisit(SQLAnyExpr x) {

    }

    @Override
    public boolean visit(SQLAnyExpr x) {
        return true;
    }

    @Override
    public void endVisit(SQLAllExpr x) {

    }

    @Override
    public boolean visit(SQLAllExpr x) {
        return true;
    }

    @Override
    public void endVisit(SQLInSubQueryExpr x) {

    }

    @Override
    public boolean visit(SQLInSubQueryExpr x) {
        return true;
    }

    @Override
    public void endVisit(SQLListExpr x) {

    }

    @Override
    public boolean visit(SQLListExpr x) {
        return true;
    }

    @Override
    public void endVisit(SQLSubqueryTableSource x) {

    }

    @Override
    public boolean visit(SQLSubqueryTableSource x) {
        return true;
    }

    @Override
    public void endVisit(SQLTruncateStatement x) {

    }

    @Override
    public boolean visit(SQLTruncateStatement x) {
        return true;
    }

    @Override
    public void endVisit(SQLDefaultExpr x) {

    }

    @Override
    public boolean visit(SQLDefaultExpr x) {
        return true;
    }

    @Override
    public void endVisit(SQLCommentStatement x) {

    }

    @Override
    public boolean visit(SQLCommentStatement x) {
        return true;
    }

    @Override
    public void endVisit(SQLUseStatement x) {

    }

    @Override
    public boolean visit(SQLUseStatement x) {
        return true;
    }

    @Override
    public boolean visit(SQLAlterTableAddColumn x) {
        return true;
    }

    @Override
    public void endVisit(SQLAlterTableAddColumn x) {

    }

    @Override
    public boolean visit(SQLAlterTableDropColumnItem x) {
        return true;
    }

    @Override
    public void endVisit(SQLAlterTableDropColumnItem x) {

    }

    @Override
    public boolean visit(SQLDropIndexStatement x) {
        return true;
    }

    @Override
    public void endVisit(SQLDropIndexStatement x) {

    }

    @Override
    public boolean visit(SQLDropViewStatement x) {
        return true;
    }

    @Override
    public void endVisit(SQLDropViewStatement x) {

    }

    @Override
    public boolean visit(SQLSavePointStatement x) {
        return true;
    }

    @Override
    public void endVisit(SQLSavePointStatement x) {

    }

    @Override
    public boolean visit(SQLRollbackStatement x) {
        return true;
    }

    @Override
    public void endVisit(SQLRollbackStatement x) {

    }

    @Override
    public boolean visit(SQLReleaseSavePointStatement x) {
        return true;
    }

    @Override
    public void endVisit(SQLReleaseSavePointStatement x) {
    }

    @Override
    public boolean visit(SQLCommentHint x) {
        return true;
    }

    @Override
    public void endVisit(SQLCommentHint x) {

    }

    @Override
    public void endVisit(SQLCreateDatabaseStatement x) {
        
    }

    @Override
    public boolean visit(SQLCreateDatabaseStatement x) {
        return true;
    }

    @Override
    public boolean visit(SQLAlterTableDropIndex x) {
        return true;
    }

    @Override
    public void endVisit(SQLAlterTableDropIndex x) {
        
    }

    @Override
    public boolean visit(SQLAlterTableAddPrimaryKey x) {
        return true;
    }

    @Override
    public void endVisit(SQLAlterTableAddPrimaryKey x) {
        
    }

    @Override
    public void endVisit(SQLOver x) {
    }

    @Override
    public boolean visit(SQLOver x) {
        return true;
    }
}
