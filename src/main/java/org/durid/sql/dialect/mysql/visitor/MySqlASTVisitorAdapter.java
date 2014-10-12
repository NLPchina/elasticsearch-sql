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

import org.durid.sql.dialect.mysql.ast.MySqlForceIndexHint;
import org.durid.sql.dialect.mysql.ast.MySqlIgnoreIndexHint;
import org.durid.sql.dialect.mysql.ast.MySqlKey;
import org.durid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import org.durid.sql.dialect.mysql.ast.MySqlUseIndexHint;
import org.durid.sql.dialect.mysql.ast.expr.MySqlBinaryExpr;
import org.durid.sql.dialect.mysql.ast.expr.MySqlBooleanExpr;
import org.durid.sql.dialect.mysql.ast.expr.MySqlCharExpr;
import org.durid.sql.dialect.mysql.ast.expr.MySqlExtractExpr;
import org.durid.sql.dialect.mysql.ast.expr.MySqlIntervalExpr;
import org.durid.sql.dialect.mysql.ast.expr.MySqlMatchAgainstExpr;
import org.durid.sql.dialect.mysql.ast.expr.MySqlOutFileExpr;
import org.durid.sql.dialect.mysql.ast.expr.MySqlUserName;
import org.durid.sql.dialect.mysql.ast.statement.CobarShowStatus;
import org.durid.sql.dialect.mysql.ast.statement.MySqlAlterTableAddColumn;
import org.durid.sql.dialect.mysql.ast.statement.MySqlAlterTableAddIndex;
import org.durid.sql.dialect.mysql.ast.statement.MySqlAlterTableAddUnique;
import org.durid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn;
import org.durid.sql.dialect.mysql.ast.statement.MySqlAlterTableCharacter;
import org.durid.sql.dialect.mysql.ast.statement.MySqlAlterTableOption;
import org.durid.sql.dialect.mysql.ast.statement.MySqlAlterTableStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlBinlogStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlCommitStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlCreateIndexStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlCreateUserStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlCreateUserStatement.UserSpecification;
import org.durid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlDescribeStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlDropTableStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlDropUser;
import org.durid.sql.dialect.mysql.ast.statement.MySqlDropViewStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlExecuteStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlHelpStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlKillStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlLoadDataInFileStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlLoadXmlStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlLockTableStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlPartitionByKey;
import org.durid.sql.dialect.mysql.ast.statement.MySqlPrepareStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlReplaceStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlResetStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlRollbackStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlSelectGroupBy;
import org.durid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import org.durid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock.Limit;
import org.durid.sql.dialect.mysql.ast.statement.MySqlSetCharSetStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlSetNamesStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlSetTransactionIsolationLevelStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowAuthorsStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowBinLogEventsStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowBinaryLogsStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowCharacterSetStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowCollationStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowColumnsStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowContributorsStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowCreateDatabaseStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowCreateEventStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowCreateFunctionStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowCreateProcedureStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowCreateTableStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowCreateTriggerStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowCreateViewStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowDatabasesStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowEngineStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowEnginesStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowErrorsStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowEventsStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowFunctionCodeStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowFunctionStatusStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowGrantsStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowIndexesStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowKeysStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowMasterLogsStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowMasterStatusStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowOpenTablesStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowPluginsStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowPrivilegesStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowProcedureCodeStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowProcedureStatusStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowProcessListStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowProfileStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowProfilesStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowRelayLogEventsStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowSlaveHostsStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowSlaveStatusStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowStatusStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowTableStatusStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowTablesStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowTriggersStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowVariantsStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlShowWarningsStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlStartTransactionStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import org.durid.sql.dialect.mysql.ast.statement.MySqlUnionQuery;
import org.durid.sql.dialect.mysql.ast.statement.MySqlUnlockTablesStatement;
import org.durid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import org.durid.sql.visitor.SQLASTVisitorAdapter;

public class MySqlASTVisitorAdapter extends SQLASTVisitorAdapter implements
		MySqlASTVisitor {

	@Override
	public boolean visit(MySqlBooleanExpr x) {
		return true;
	}

	@Override
	public void endVisit(MySqlBooleanExpr x) {

	}

	@Override
	public boolean visit(Limit x) {
		return true;
	}

	@Override
	public void endVisit(Limit x) {

	}

	@Override
	public boolean visit(MySqlTableIndex x) {
		return true;
	}

	@Override
	public void endVisit(MySqlTableIndex x) {

	}

	@Override
	public boolean visit(MySqlKey x) {
		return true;
	}

	@Override
	public void endVisit(MySqlKey x) {

	}

	@Override
	public boolean visit(MySqlPrimaryKey x) {

		return true;
	}

	@Override
	public void endVisit(MySqlPrimaryKey x) {

	}

	@Override
	public void endVisit(MySqlIntervalExpr x) {

	}

	@Override
	public boolean visit(MySqlIntervalExpr x) {

		return true;
	}

	@Override
	public void endVisit(MySqlExtractExpr x) {

	}

	@Override
	public boolean visit(MySqlExtractExpr x) {

		return true;
	}

	@Override
	public void endVisit(MySqlMatchAgainstExpr x) {

	}

	@Override
	public boolean visit(MySqlMatchAgainstExpr x) {

		return true;
	}

	@Override
	public void endVisit(MySqlBinaryExpr x) {

	}

	@Override
	public boolean visit(MySqlBinaryExpr x) {

		return true;
	}

	@Override
	public void endVisit(MySqlPrepareStatement x) {

	}

	@Override
	public boolean visit(MySqlPrepareStatement x) {

		return true;
	}

	@Override
	public void endVisit(MySqlExecuteStatement x) {

	}

	@Override
	public boolean visit(MySqlExecuteStatement x) {

		return true;
	}

	@Override
	public void endVisit(MySqlDeleteStatement x) {

	}

	@Override
	public boolean visit(MySqlDeleteStatement x) {

		return true;
	}

	@Override
	public void endVisit(MySqlInsertStatement x) {

	}

	@Override
	public boolean visit(MySqlInsertStatement x) {

		return true;
	}

	@Override
	public void endVisit(MySqlLoadDataInFileStatement x) {

	}

	@Override
	public boolean visit(MySqlLoadDataInFileStatement x) {

		return true;
	}

	@Override
	public void endVisit(MySqlLoadXmlStatement x) {

	}

	@Override
	public boolean visit(MySqlLoadXmlStatement x) {

		return true;
	}

	@Override
	public void endVisit(MySqlReplaceStatement x) {

	}

	@Override
	public boolean visit(MySqlReplaceStatement x) {

		return true;
	}

	@Override
	public void endVisit(MySqlSelectGroupBy x) {

	}

	@Override
	public boolean visit(MySqlSelectGroupBy x) {

		return true;
	}

	@Override
	public void endVisit(MySqlStartTransactionStatement x) {

	}

	@Override
	public boolean visit(MySqlStartTransactionStatement x) {

		return true;
	}

	@Override
	public void endVisit(MySqlCommitStatement x) {

	}

	@Override
	public boolean visit(MySqlCommitStatement x) {

		return true;
	}

	@Override
	public void endVisit(MySqlRollbackStatement x) {

	}

	@Override
	public boolean visit(MySqlRollbackStatement x) {

		return true;
	}

	@Override
	public void endVisit(MySqlShowColumnsStatement x) {

	}

	@Override
	public boolean visit(MySqlShowColumnsStatement x) {

		return true;
	}

	@Override
	public void endVisit(MySqlShowTablesStatement x) {

	}

	@Override
	public boolean visit(MySqlShowTablesStatement x) {

		return true;
	}

	@Override
	public void endVisit(MySqlShowDatabasesStatement x) {

	}

	@Override
	public boolean visit(MySqlShowDatabasesStatement x) {

		return true;
	}

	@Override
	public void endVisit(MySqlShowWarningsStatement x) {

	}

	@Override
	public boolean visit(MySqlShowWarningsStatement x) {

		return true;
	}

	@Override
	public void endVisit(MySqlShowStatusStatement x) {

	}

	@Override
	public boolean visit(MySqlShowStatusStatement x) {

		return true;
	}

	@Override
	public void endVisit(CobarShowStatus x) {

	}

	@Override
	public boolean visit(CobarShowStatus x) {
		return true;
	}

	@Override
	public void endVisit(MySqlKillStatement x) {

	}

	@Override
	public boolean visit(MySqlKillStatement x) {
		return true;
	}

	@Override
	public void endVisit(MySqlBinlogStatement x) {

	}

	@Override
	public boolean visit(MySqlBinlogStatement x) {
		return true;
	}
	
	@Override
	public void endVisit(MySqlResetStatement x) {
		
	}
	
	@Override
	public boolean visit(MySqlResetStatement x) {
		return true;
	}

    @Override
    public void endVisit(MySqlCreateUserStatement x) {
        
    }

    @Override
    public boolean visit(MySqlCreateUserStatement x) {
        return true;
    }

    @Override
    public void endVisit(UserSpecification x) {
        
    }

    @Override
    public boolean visit(UserSpecification x) {
        return true;
    }
    
    @Override
    public void endVisit(MySqlDropUser x) {
        
    }
    
    @Override
    public boolean visit(MySqlDropUser x) {
        return true;
    }
    
    @Override
    public void endVisit(MySqlDropTableStatement x) {
        
    }
    
    @Override
    public boolean visit(MySqlDropTableStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlPartitionByKey x) {
        
    }

    @Override
    public boolean visit(MySqlPartitionByKey x) {
        return true;
    }

    @Override
    public boolean visit(MySqlSelectQueryBlock x) {
        return true;
    }

    @Override
    public void endVisit(MySqlSelectQueryBlock x) {
        
    }

    @Override
    public boolean visit(MySqlOutFileExpr x) {
        return true;
    }

    @Override
    public void endVisit(MySqlOutFileExpr x) {
        
    }

    @Override
    public boolean visit(MySqlDescribeStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlDescribeStatement x) {
        
    }

	@Override
	public boolean visit(MySqlUpdateStatement x) {
		return true;
	}

	@Override
	public void endVisit(MySqlUpdateStatement x) {
		
	}

    @Override
    public boolean visit(MySqlSetTransactionIsolationLevelStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlSetTransactionIsolationLevelStatement x) {
        
    }
    
    @Override
    public boolean visit(MySqlSetNamesStatement x) {
        return true;
    }
    
    @Override
    public void endVisit(MySqlSetNamesStatement x) {
        
    }
    
    @Override
    public boolean visit(MySqlSetCharSetStatement x) {
        return true;
    }
    
    @Override
    public void endVisit(MySqlSetCharSetStatement x) {
        
    }
    
    @Override
    public boolean visit(MySqlShowAuthorsStatement x) {
        return true;
    }
    
    @Override
    public void endVisit(MySqlShowAuthorsStatement x) {
        
    }
    
    @Override
    public boolean visit(MySqlShowBinaryLogsStatement x) {
        return true;
    }
    
    @Override
    public void endVisit(MySqlShowBinaryLogsStatement x) {
        
    }
    
    @Override
    public boolean visit(MySqlShowMasterLogsStatement x) {
        return true;
    }
    
    @Override
    public void endVisit(MySqlShowMasterLogsStatement x) {
        
    }
    
    @Override
    public boolean visit(MySqlShowCollationStatement x) {
        return true;
    }
    
    @Override
    public void endVisit(MySqlShowCollationStatement x) {
        
    }
    
    @Override
    public boolean visit(MySqlShowBinLogEventsStatement x) {
        return true;
    }
    
    @Override
    public void endVisit(MySqlShowBinLogEventsStatement x) {
        
    }

    @Override
    public boolean visit(MySqlShowCharacterSetStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowCharacterSetStatement x) {
        
    }
    
    @Override
    public boolean visit(MySqlShowContributorsStatement x) {
        return true;
    }
    
    @Override
    public void endVisit(MySqlShowContributorsStatement x) {
        
    }
    
    @Override
    public boolean visit(MySqlShowCreateDatabaseStatement x) {
        return true;
    }
    
    @Override
    public void endVisit(MySqlShowCreateDatabaseStatement x) {
        
    }
    
    @Override
    public boolean visit(MySqlShowCreateEventStatement x) {
        return true;
    }
    
    @Override
    public void endVisit(MySqlShowCreateEventStatement x) {
        
    }
    
    @Override
    public boolean visit(MySqlShowCreateFunctionStatement x) {
        return true;
    }
    
    @Override
    public void endVisit(MySqlShowCreateFunctionStatement x) {
        
    }

    @Override
    public boolean visit(MySqlShowCreateProcedureStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowCreateProcedureStatement x) {
        
    }
    
    @Override
    public boolean visit(MySqlShowCreateTableStatement x) {
        return true;
    }
    
    @Override
    public void endVisit(MySqlShowCreateTableStatement x) {
        
    }
    
    @Override
    public boolean visit(MySqlShowCreateTriggerStatement x) {
        return true;
    }
    
    @Override
    public void endVisit(MySqlShowCreateTriggerStatement x) {
        
    }

    @Override
    public boolean visit(MySqlShowCreateViewStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowCreateViewStatement x) {
        
    }

    @Override
    public boolean visit(MySqlShowEngineStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowEngineStatement x) {
        
    }
    
    @Override
    public boolean visit(MySqlShowEnginesStatement x) {
        return true;
    }
    
    @Override
    public void endVisit(MySqlShowEnginesStatement x) {
        
    }

    @Override
    public boolean visit(MySqlShowErrorsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowErrorsStatement x) {
        
    }
    
    @Override
    public boolean visit(MySqlShowEventsStatement x) {
        return true;
    }
    
    @Override
    public void endVisit(MySqlShowEventsStatement x) {
        
    }

    @Override
    public boolean visit(MySqlShowFunctionCodeStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowFunctionCodeStatement x) {
        
    }

    @Override
    public boolean visit(MySqlShowFunctionStatusStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowFunctionStatusStatement x) {
        
    }

    @Override
    public boolean visit(MySqlShowGrantsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowGrantsStatement x) {
    }

    @Override
    public boolean visit(MySqlUserName x) {
        return true;
    }

    @Override
    public void endVisit(MySqlUserName x) {
        
    }

    @Override
    public boolean visit(MySqlShowIndexesStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowIndexesStatement x) {
        
    }

    @Override
    public boolean visit(MySqlShowKeysStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowKeysStatement x) {
        
    }
    
    @Override
    public boolean visit(MySqlShowMasterStatusStatement x) {
        return true;
    }
    
    @Override
    public void endVisit(MySqlShowMasterStatusStatement x) {
        
    }

    @Override
    public boolean visit(MySqlShowOpenTablesStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowOpenTablesStatement x) {
        
    }

    @Override
    public boolean visit(MySqlShowPluginsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowPluginsStatement x) {
        
    }
    
    @Override
    public boolean visit(MySqlShowPrivilegesStatement x) {
        return true;
    }
    
    @Override
    public void endVisit(MySqlShowPrivilegesStatement x) {
        
    }

    @Override
    public boolean visit(MySqlShowProcedureCodeStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowProcedureCodeStatement x) {
        
    }

    @Override
    public boolean visit(MySqlShowProcedureStatusStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowProcedureStatusStatement x) {
        
    }

    @Override
    public boolean visit(MySqlShowProcessListStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowProcessListStatement x) {
        
    }

    @Override
    public boolean visit(MySqlShowProfileStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowProfileStatement x) {
        
    }

    @Override
    public boolean visit(MySqlShowProfilesStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowProfilesStatement x) {
        
    }

    @Override
    public boolean visit(MySqlShowRelayLogEventsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowRelayLogEventsStatement x) {
        
    }

    @Override
    public boolean visit(MySqlShowSlaveHostsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowSlaveHostsStatement x) {
        
    }
    
    @Override
    public boolean visit(MySqlShowSlaveStatusStatement x) {
        return true;
    }
    
    @Override
    public void endVisit(MySqlShowSlaveStatusStatement x) {
        
    }

    @Override
    public boolean visit(MySqlShowTableStatusStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowTableStatusStatement x) {
        
    }

    @Override
    public boolean visit(MySqlShowTriggersStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowTriggersStatement x) {
        
    }

    @Override
    public boolean visit(MySqlShowVariantsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowVariantsStatement x) {
        
    }

    @Override
    public boolean visit(MySqlAlterTableStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAlterTableStatement x) {
        
    }

    @Override
    public boolean visit(MySqlAlterTableAddColumn x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAlterTableAddColumn x) {
        
    }
    
    @Override
    public boolean visit(MySqlCreateIndexStatement x) {
        return true;
    }
    
    @Override
    public void endVisit(MySqlCreateIndexStatement x) {
        
    }

    @Override
    public boolean visit(MySqlRenameTableStatement.Item x) {
        return true;
    }

    @Override
    public void endVisit(MySqlRenameTableStatement.Item x) {
        
    }

    @Override
    public boolean visit(MySqlRenameTableStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlRenameTableStatement x) {
        
    }

    @Override
    public boolean visit(MySqlDropViewStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlDropViewStatement x) {
        
    }

    @Override
    public boolean visit(MySqlUnionQuery x) {
        return true;
    }

    @Override
    public void endVisit(MySqlUnionQuery x) {
        
    }

    @Override
    public boolean visit(MySqlUseIndexHint x) {
        return true;
    }

    @Override
    public void endVisit(MySqlUseIndexHint x) {
        
    }

    @Override
    public boolean visit(MySqlIgnoreIndexHint x) {
        return true;
    }

    @Override
    public void endVisit(MySqlIgnoreIndexHint x) {
        
    }

    @Override
    public boolean visit(MySqlLockTableStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlLockTableStatement x) {
        
    }

    @Override
    public boolean visit(MySqlUnlockTablesStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlUnlockTablesStatement x) {
        
    }

    @Override
    public boolean visit(MySqlForceIndexHint x) {
        return true;
    }

    @Override
    public void endVisit(MySqlForceIndexHint x) {
        
    }

    @Override
    public boolean visit(MySqlAlterTableChangeColumn x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAlterTableChangeColumn x) {
        
    }

    @Override
    public boolean visit(MySqlAlterTableCharacter x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAlterTableCharacter x) {
        
    }

    @Override
    public boolean visit(MySqlAlterTableAddIndex x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAlterTableAddIndex x) {
        
    }

    @Override
    public boolean visit(MySqlAlterTableOption x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAlterTableOption x) {
        
    }

    @Override
    public boolean visit(MySqlCreateTableStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlCreateTableStatement x) {
        
    }

    @Override
    public boolean visit(MySqlHelpStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlHelpStatement x) {
        
    }

    @Override
    public boolean visit(MySqlCharExpr x) {
        return true;
    }

    @Override
    public void endVisit(MySqlCharExpr x) {
        
    }

    @Override
    public boolean visit(MySqlAlterTableAddUnique x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAlterTableAddUnique x) {
        
    }
}
