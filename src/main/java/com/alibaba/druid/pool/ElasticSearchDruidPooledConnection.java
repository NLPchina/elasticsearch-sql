package com.alibaba.druid.pool;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by allwefantasy on 8/30/16.
 */
public class ElasticSearchDruidPooledConnection extends DruidPooledConnection {
    public ElasticSearchDruidPooledConnection(DruidConnectionHolder holder) {
        super(holder);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkState();

        PreparedStatementHolder stmtHolder = null;
        DruidPooledPreparedStatement.PreparedStatementKey key = new DruidPooledPreparedStatement.PreparedStatementKey(sql, getCatalog(), PreparedStatementPool.MethodType.M1);

        boolean poolPreparedStatements = holder.isPoolPreparedStatements();

        if (poolPreparedStatements) {
            stmtHolder = holder.getStatementPool().get(key);
        }

        if (stmtHolder == null) {
            try {
                stmtHolder = new PreparedStatementHolder(key, conn.prepareStatement(sql));
                holder.getDataSource().incrementPreparedStatementCount();
            } catch (SQLException ex) {
                handleException(ex, sql);
            }
        }

        initStatement(stmtHolder);

        DruidPooledPreparedStatement rtnVal = new ElasticSearchDruidPooledPreparedStatement(this, stmtHolder);

        holder.addTrace(rtnVal);

        return rtnVal;
    }


    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        checkState();

        PreparedStatementHolder stmtHolder = null;
        DruidPooledPreparedStatement.PreparedStatementKey key = new DruidPooledPreparedStatement.PreparedStatementKey(sql, getCatalog(), PreparedStatementPool.MethodType.M2, resultSetType,
                resultSetConcurrency);

        boolean poolPreparedStatements = holder.isPoolPreparedStatements();

        if (poolPreparedStatements) {
            stmtHolder = holder.getStatementPool().get(key);
        }

        if (stmtHolder == null) {
            try {
                stmtHolder = new PreparedStatementHolder(key, conn.prepareStatement(sql, resultSetType,
                        resultSetConcurrency));
                holder.getDataSource().incrementPreparedStatementCount();
            } catch (SQLException ex) {
                handleException(ex, sql);
            }
        }

        initStatement(stmtHolder);

        DruidPooledPreparedStatement rtnVal = new ElasticSearchDruidPooledPreparedStatement(this, stmtHolder);

        holder.addTrace(rtnVal);

        return rtnVal;
    }

    private void initStatement(PreparedStatementHolder stmtHolder) throws SQLException {
        stmtHolder.incrementInUseCount();
        holder.getDataSource().initStatement(this, stmtHolder.statement);
    }

}
