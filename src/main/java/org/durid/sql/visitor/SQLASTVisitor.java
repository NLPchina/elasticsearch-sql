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

public interface SQLASTVisitor {

    void endVisit(SQLAllColumnExpr x);

    void endVisit(SQLBetweenExpr x);

    void endVisit(SQLBinaryOpExpr x);

    void endVisit(SQLCaseExpr x);

    void endVisit(SQLCaseExpr.Item x);

    void endVisit(SQLCharExpr x);

    void endVisit(SQLIdentifierExpr x);

    void endVisit(SQLInListExpr x);

    void endVisit(SQLIntegerExpr x);

    void endVisit(SQLExistsExpr x);

    void endVisit(SQLNCharExpr x);

    void endVisit(SQLNotExpr x);

    void endVisit(SQLNullExpr x);

    void endVisit(SQLNumberExpr x);

    void endVisit(SQLPropertyExpr x);

    void endVisit(SQLSelectGroupByClause x);

    void endVisit(SQLSelectItem x);

    void endVisit(SQLSelectStatement selectStatement);

    void postVisit(SQLObject astNode);

    void preVisit(SQLObject astNode);

    boolean visit(SQLAllColumnExpr x);

    boolean visit(SQLBetweenExpr x);

    boolean visit(SQLBinaryOpExpr x);

    boolean visit(SQLCaseExpr x);

    boolean visit(SQLCaseExpr.Item x);

    boolean visit(SQLCastExpr x);

    boolean visit(SQLCharExpr x);

    boolean visit(SQLExistsExpr x);

    boolean visit(SQLIdentifierExpr x);

    boolean visit(SQLInListExpr x);

    boolean visit(SQLIntegerExpr x);

    boolean visit(SQLNCharExpr x);

    boolean visit(SQLNotExpr x);

    boolean visit(SQLNullExpr x);

    boolean visit(SQLNumberExpr x);

    boolean visit(SQLPropertyExpr x);

    boolean visit(SQLSelectGroupByClause x);

    boolean visit(SQLSelectItem x);

    void endVisit(SQLCastExpr x);

    boolean visit(SQLSelectStatement astNode);

    void endVisit(SQLAggregateExpr astNode);

    boolean visit(SQLAggregateExpr astNode);

    boolean visit(SQLVariantRefExpr x);

    void endVisit(SQLVariantRefExpr x);

    boolean visit(SQLQueryExpr x);

    void endVisit(SQLQueryExpr x);

    boolean visit(SQLUnaryExpr x);

    void endVisit(SQLUnaryExpr x);

    boolean visit(SQLHexExpr x);

    void endVisit(SQLHexExpr x);

    boolean visit(SQLBitStringLiteralExpr x);

    void endVisit(SQLBitStringLiteralExpr x);

    boolean visit(SQLHexStringLiteralExpr x);

    void endVisit(SQLHexStringLiteralExpr x);

    boolean visit(SQLDateLiteralExpr x);

    void endVisit(SQLDateLiteralExpr x);

    boolean visit(SQLSelect x);

    void endVisit(SQLSelect select);

    boolean visit(SQLSelectQueryBlock x);

    void endVisit(SQLSelectQueryBlock x);

    boolean visit(SQLExprTableSource x);

    void endVisit(SQLExprTableSource x);

    boolean visit(SQLIntervalLiteralExpr x);

    void endVisit(SQLIntervalLiteralExpr x);

    boolean visit(SQLOrderBy x);

    void endVisit(SQLOrderBy x);

    boolean visit(SQLSelectOrderByItem x);

    void endVisit(SQLSelectOrderByItem x);

    boolean visit(SQLDropTableStatement x);

    void endVisit(SQLDropTableStatement x);

    boolean visit(SQLCreateTableStatement x);

    void endVisit(SQLCreateTableStatement x);

    boolean visit(SQLTableElement x);

    void endVisit(SQLTableElement x);

    boolean visit(SQLColumnDefinition x);

    void endVisit(SQLColumnDefinition x);

    boolean visit(SQLDataType x);

    void endVisit(SQLDataType x);

    boolean visit(SQLDeleteStatement x);

    void endVisit(SQLDeleteStatement x);

    boolean visit(SQLCurrentOfCursorExpr x);

    void endVisit(SQLCurrentOfCursorExpr x);

    boolean visit(SQLInsertStatement x);

    void endVisit(SQLInsertStatement x);

    boolean visit(SQLInsertStatement.ValuesClause x);

    void endVisit(SQLInsertStatement.ValuesClause x);

    boolean visit(SQLUpdateSetItem x);

    void endVisit(SQLUpdateSetItem x);

    boolean visit(SQLUpdateStatement x);

    void endVisit(SQLUpdateStatement x);

    boolean visit(SQLCreateViewStatement x);

    void endVisit(SQLCreateViewStatement x);

    boolean visit(SQLUniqueConstraint x);

    void endVisit(SQLUniqueConstraint x);

    boolean visit(NotNullConstraint x);

    void endVisit(NotNullConstraint x);

    void endVisit(SQLMethodInvokeExpr x);

    boolean visit(SQLMethodInvokeExpr x);

    void endVisit(SQLUnionQuery x);

    boolean visit(SQLUnionQuery x);

    void endVisit(SQLSetStatement x);

    boolean visit(SQLSetStatement x);

    void endVisit(SQLAssignItem x);

    boolean visit(SQLAssignItem x);

    void endVisit(SQLCallStatement x);

    boolean visit(SQLCallStatement x);

    void endVisit(SQLJoinTableSource x);

    boolean visit(SQLJoinTableSource x);

    void endVisit(SQLSomeExpr x);

    boolean visit(SQLSomeExpr x);

    void endVisit(SQLAnyExpr x);

    boolean visit(SQLAnyExpr x);

    void endVisit(SQLAllExpr x);

    boolean visit(SQLAllExpr x);

    void endVisit(SQLInSubQueryExpr x);

    boolean visit(SQLInSubQueryExpr x);

    void endVisit(SQLListExpr x);

    boolean visit(SQLListExpr x);

    void endVisit(SQLSubqueryTableSource x);

    boolean visit(SQLSubqueryTableSource x);

    void endVisit(SQLTruncateStatement x);

    boolean visit(SQLTruncateStatement x);

    void endVisit(SQLDefaultExpr x);

    boolean visit(SQLDefaultExpr x);

    void endVisit(SQLCommentStatement x);

    boolean visit(SQLCommentStatement x);

    void endVisit(SQLUseStatement x);

    boolean visit(SQLUseStatement x);

    boolean visit(SQLAlterTableAddColumn x);

    void endVisit(SQLAlterTableAddColumn x);

    boolean visit(SQLAlterTableDropColumnItem x);

    void endVisit(SQLAlterTableDropColumnItem x);
    
    boolean visit(SQLAlterTableDropIndex x);
    
    void endVisit(SQLAlterTableDropIndex x);
    
    boolean visit(SQLAlterTableAddPrimaryKey x);
    
    void endVisit(SQLAlterTableAddPrimaryKey x);

    boolean visit(SQLDropIndexStatement x);

    void endVisit(SQLDropIndexStatement x);

    boolean visit(SQLDropViewStatement x);

    void endVisit(SQLDropViewStatement x);

    boolean visit(SQLSavePointStatement x);

    void endVisit(SQLSavePointStatement x);

    boolean visit(SQLRollbackStatement x);

    void endVisit(SQLRollbackStatement x);

    boolean visit(SQLReleaseSavePointStatement x);

    void endVisit(SQLReleaseSavePointStatement x);

    void endVisit(SQLCommentHint x);

    boolean visit(SQLCommentHint x);

    void endVisit(SQLCreateDatabaseStatement x);

    boolean visit(SQLCreateDatabaseStatement x);
    
    void endVisit(SQLOver x);
    
    boolean visit(SQLOver x);
}
