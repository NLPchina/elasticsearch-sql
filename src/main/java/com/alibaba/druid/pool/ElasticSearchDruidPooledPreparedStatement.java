package com.alibaba.druid.pool;

import org.elasticsearch.client.Client;
import org.elasticsearch.plugin.nlpcn.QueryActionElasticExecutor;
import org.elasticsearch.plugin.nlpcn.executors.CsvExtractorException;
import org.nlpcn.es4sql.SearchDao;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.jdbc.ObjectResult;
import org.nlpcn.es4sql.jdbc.ObjectResultsExtractor;
import org.nlpcn.es4sql.query.QueryAction;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;

/**
 * Created by allwefantasy on 8/30/16.
 */
public class ElasticSearchDruidPooledPreparedStatement extends DruidPooledPreparedStatement {

    private final Client client;

    public ElasticSearchDruidPooledPreparedStatement(DruidPooledConnection conn, PreparedStatementHolder holder) throws SQLException {
        super(conn, holder);
        this.client = ((ElasticSearchConnection) conn.getConnection()).getClient();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        checkOpen();

        incrementExecuteQueryCount();
        transactionRecord(getSql());

        oracleSetRowPrefetch();

        conn.beforeExecute();
        try {
            ObjectResult extractor = getObjectResult(true, false, false, true);
            List<String> headers = extractor.getHeaders();
            List<List<Object>> lines = extractor.getLines();

            ResultSet rs = new ElasticSearchResultSet(this, headers, lines);

            DruidPooledResultSet poolableResultSet = new DruidPooledResultSet(this, rs);
            addResultSetTrace(poolableResultSet);

            return poolableResultSet;
        } catch (Throwable t) {
            errorCheck(t);

            throw checkException(t);
        } finally {
            conn.afterExecute();
        }
    }

    @Override
    public boolean execute() throws SQLException {
        checkOpen();

        incrementExecuteCount();
        transactionRecord(getSql());

        // oracleSetRowPrefetch();

        conn.beforeExecute();
        try {
            ObjectResult extractor = getObjectResult(true, false, false, true);
            List<String> headers = extractor.getHeaders();
            List<List<Object>> lines = extractor.getLines();

            ResultSet rs = new ElasticSearchResultSet(this, headers, lines);
            ((ElasticSearchPreparedStatement) getRawPreparedStatement()).setResults(rs);

            return true;
        } catch (Throwable t) {
            errorCheck(t);

            throw checkException(t);
        } finally {
            conn.afterExecute();
        }
    }

    private ObjectResult getObjectResult(boolean flat, boolean includeScore, boolean includeType, boolean includeId) throws SqlParseException, SQLFeatureNotSupportedException, Exception, CsvExtractorException {
        SearchDao searchDao = new org.nlpcn.es4sql.SearchDao(client);

        String query = ((ElasticSearchPreparedStatement) getRawPreparedStatement()).getExecutableSql();
        QueryAction queryAction = searchDao.explain(query);
        Object execution = QueryActionElasticExecutor.executeAnyAction(searchDao.getClient(), queryAction);
        return new ObjectResultsExtractor(includeScore, includeType, includeId, false, queryAction).extractResults(execution, flat);
    }

    @Override
    public int executeUpdate() throws SQLException {
        throw new SQLException("executeUpdate not support in ElasticSearch");
    }
}
