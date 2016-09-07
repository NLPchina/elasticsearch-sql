package com.alibaba.druid.pool;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.Executor;

/**
 * Created by allwefantasy on 8/30/16.
 */
public class ElasticSearchConnection implements Connection {

    private Client client;

    public ElasticSearchConnection(String jdbcUrl) {


        Settings settings = Settings.builder().put("client.transport.ignore_cluster_name", true).build();
        try {
            TransportClient transportClient = TransportClient.builder().settings(settings).build();

            String hostAndPortArrayStr = jdbcUrl.split("/")[2];
            String[] hostAndPortArray = hostAndPortArrayStr.split(",");

            for (String hostAndPort : hostAndPortArray) {
                String host = hostAndPort.split(":")[0];
                String port = hostAndPort.split(":")[1];
                transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), Integer.parseInt(port)));
            }
            client = transportClient;
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public Client getClient() {
        return client;
    }

    @Override
    public Statement createStatement() throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new PreparedStatement() {
            @Override
            public ResultSet executeQuery() throws SQLException {
                return null;
            }

            @Override
            public int executeUpdate() throws SQLException {
                return 0;
            }

            @Override
            public void setNull(int parameterIndex, int sqlType) throws SQLException {

            }

            @Override
            public void setBoolean(int parameterIndex, boolean x) throws SQLException {

            }

            @Override
            public void setByte(int parameterIndex, byte x) throws SQLException {

            }

            @Override
            public void setShort(int parameterIndex, short x) throws SQLException {

            }

            @Override
            public void setInt(int parameterIndex, int x) throws SQLException {

            }

            @Override
            public void setLong(int parameterIndex, long x) throws SQLException {

            }

            @Override
            public void setFloat(int parameterIndex, float x) throws SQLException {

            }

            @Override
            public void setDouble(int parameterIndex, double x) throws SQLException {

            }

            @Override
            public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {

            }

            @Override
            public void setString(int parameterIndex, String x) throws SQLException {

            }

            @Override
            public void setBytes(int parameterIndex, byte[] x) throws SQLException {

            }

            @Override
            public void setDate(int parameterIndex, Date x) throws SQLException {

            }

            @Override
            public void setTime(int parameterIndex, Time x) throws SQLException {

            }

            @Override
            public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {

            }

            @Override
            public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {

            }

            @Override
            public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {

            }

            @Override
            public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {

            }

            @Override
            public void clearParameters() throws SQLException {

            }

            @Override
            public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {

            }

            @Override
            public void setObject(int parameterIndex, Object x) throws SQLException {

            }

            @Override
            public boolean execute() throws SQLException {
                return false;
            }

            @Override
            public void addBatch() throws SQLException {

            }

            @Override
            public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {

            }

            @Override
            public void setRef(int parameterIndex, Ref x) throws SQLException {

            }

            @Override
            public void setBlob(int parameterIndex, Blob x) throws SQLException {

            }

            @Override
            public void setClob(int parameterIndex, Clob x) throws SQLException {

            }

            @Override
            public void setArray(int parameterIndex, Array x) throws SQLException {

            }

            @Override
            public ResultSetMetaData getMetaData() throws SQLException {
                return null;
            }

            @Override
            public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {

            }

            @Override
            public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {

            }

            @Override
            public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {

            }

            @Override
            public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {

            }

            @Override
            public void setURL(int parameterIndex, URL x) throws SQLException {

            }

            @Override
            public ParameterMetaData getParameterMetaData() throws SQLException {
                return null;
            }

            @Override
            public void setRowId(int parameterIndex, RowId x) throws SQLException {

            }

            @Override
            public void setNString(int parameterIndex, String value) throws SQLException {

            }

            @Override
            public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {

            }

            @Override
            public void setNClob(int parameterIndex, NClob value) throws SQLException {

            }

            @Override
            public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {

            }

            @Override
            public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {

            }

            @Override
            public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {

            }

            @Override
            public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {

            }

            @Override
            public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {

            }

            @Override
            public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {

            }

            @Override
            public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {

            }

            @Override
            public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {

            }

            @Override
            public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {

            }

            @Override
            public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {

            }

            @Override
            public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {

            }

            @Override
            public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {

            }

            @Override
            public void setClob(int parameterIndex, Reader reader) throws SQLException {

            }

            @Override
            public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {

            }

            @Override
            public void setNClob(int parameterIndex, Reader reader) throws SQLException {

            }

            @Override
            public ResultSet executeQuery(String sql) throws SQLException {
                return null;
            }

            @Override
            public int executeUpdate(String sql) throws SQLException {
                return 0;
            }

            @Override
            public void close() throws SQLException {
                client.close();
            }

            @Override
            public int getMaxFieldSize() throws SQLException {
                return 0;
            }

            @Override
            public void setMaxFieldSize(int max) throws SQLException {

            }

            @Override
            public int getMaxRows() throws SQLException {
                return 0;
            }

            @Override
            public void setMaxRows(int max) throws SQLException {

            }

            @Override
            public void setEscapeProcessing(boolean enable) throws SQLException {

            }

            @Override
            public int getQueryTimeout() throws SQLException {
                return 0;
            }

            @Override
            public void setQueryTimeout(int seconds) throws SQLException {

            }

            @Override
            public void cancel() throws SQLException {

            }

            @Override
            public SQLWarning getWarnings() throws SQLException {
                return null;
            }

            @Override
            public void clearWarnings() throws SQLException {

            }

            @Override
            public void setCursorName(String name) throws SQLException {

            }

            @Override
            public boolean execute(String sql) throws SQLException {
                return false;
            }

            @Override
            public ResultSet getResultSet() throws SQLException {
                return null;
            }

            @Override
            public int getUpdateCount() throws SQLException {
                return 0;
            }

            @Override
            public boolean getMoreResults() throws SQLException {
                return false;
            }

            @Override
            public void setFetchDirection(int direction) throws SQLException {

            }

            @Override
            public int getFetchDirection() throws SQLException {
                return 0;
            }

            @Override
            public void setFetchSize(int rows) throws SQLException {

            }

            @Override
            public int getFetchSize() throws SQLException {
                return 0;
            }

            @Override
            public int getResultSetConcurrency() throws SQLException {
                return 0;
            }

            @Override
            public int getResultSetType() throws SQLException {
                return 0;
            }

            @Override
            public void addBatch(String sql) throws SQLException {

            }

            @Override
            public void clearBatch() throws SQLException {

            }

            @Override
            public int[] executeBatch() throws SQLException {
                return new int[0];
            }

            @Override
            public Connection getConnection() throws SQLException {
                return null;
            }

            @Override
            public boolean getMoreResults(int current) throws SQLException {
                return false;
            }

            @Override
            public ResultSet getGeneratedKeys() throws SQLException {
                return null;
            }

            @Override
            public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
                return 0;
            }

            @Override
            public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
                return 0;
            }

            @Override
            public int executeUpdate(String sql, String[] columnNames) throws SQLException {
                return 0;
            }

            @Override
            public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
                return false;
            }

            @Override
            public boolean execute(String sql, int[] columnIndexes) throws SQLException {
                return false;
            }

            @Override
            public boolean execute(String sql, String[] columnNames) throws SQLException {
                return false;
            }

            @Override
            public int getResultSetHoldability() throws SQLException {
                return 0;
            }

            @Override
            public boolean isClosed() throws SQLException {
                return false;
            }

            @Override
            public void setPoolable(boolean poolable) throws SQLException {

            }

            @Override
            public boolean isPoolable() throws SQLException {
                return false;
            }

            @Override
            public void closeOnCompletion() throws SQLException {

            }

            @Override
            public boolean isCloseOnCompletion() throws SQLException {
                return false;
            }

            @Override
            public <T> T unwrap(Class<T> iface) throws SQLException {
                return null;
            }

            @Override
            public boolean isWrapperFor(Class<?> iface) throws SQLException {
                return false;
            }
        };
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return null;
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return null;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {

    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return false;
    }

    @Override
    public void commit() throws SQLException {

    }

    @Override
    public void rollback() throws SQLException {

    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {

    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {

    }

    @Override
    public String getCatalog() throws SQLException {
        return null;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {

    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return 0;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return null;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return null;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

    }

    @Override
    public void setHoldability(int holdability) throws SQLException {

    }

    @Override
    public int getHoldability() throws SQLException {
        return 0;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return null;
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return null;
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {

    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {

    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return null;
    }

    @Override
    public Clob createClob() throws SQLException {
        return null;
    }

    @Override
    public Blob createBlob() throws SQLException {
        return null;
    }

    @Override
    public NClob createNClob() throws SQLException {
        return null;
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return null;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return false;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {

    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {

    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return null;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return null;
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return null;
    }

    @Override
    public void setSchema(String schema) throws SQLException {

    }

    @Override
    public String getSchema() throws SQLException {
        return null;
    }

    @Override
    public void abort(Executor executor) throws SQLException {

    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {

    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return 0;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
