package com.alibaba.druid.pool;

import com.alibaba.druid.proxy.jdbc.JdbcParameter;
import com.alibaba.druid.proxy.jdbc.JdbcParameterDate;
import com.alibaba.druid.proxy.jdbc.JdbcParameterDecimal;
import com.alibaba.druid.proxy.jdbc.JdbcParameterImpl;
import com.alibaba.druid.proxy.jdbc.JdbcParameterInt;
import com.alibaba.druid.proxy.jdbc.JdbcParameterLong;
import com.alibaba.druid.proxy.jdbc.JdbcParameterNull;
import com.alibaba.druid.proxy.jdbc.JdbcParameterString;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.support.logging.Log;
import com.alibaba.druid.support.logging.LogFactory;
import com.alibaba.druid.util.JdbcConstants;
import org.nlpcn.es4sql.query.ESActionFactory;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

public class ElasticSearchPreparedStatement implements PreparedStatement {

    private final static Log LOG = LogFactory.getLog(ElasticSearchPreparedStatement.class);

    private String sql;
    private JdbcParameter[] parameters;
    private int parametersSize;
    private ResultSet results = null;

    private SQLUtils.FormatOption sqlFormatOption = new SQLUtils.FormatOption(false, false);

    public ElasticSearchPreparedStatement(String sql) {
        this.sql = sql;
        this.parameters = new JdbcParameter[16];
        this.parametersSize = 0;
    }

    public String getSql() {
        return sql;
    }

    public String getExecutableSql() {
        if (parametersSize < 1) {
            return sql;
        }

        List<Object> parameters = new ArrayList<>(parametersSize);
        JdbcParameter jdbcParam;
        for (int i = 0; i < parametersSize; ++i) {
            jdbcParam = this.parameters[i];
            parameters.add(jdbcParam != null ? jdbcParam.getValue() : null);
        }

        try {
            SQLStatementParser parser = ESActionFactory.createSqlStatementParser(sql);
            List<SQLStatement> statementList = parser.parseStatementList();
            return SQLUtils.toSQLString(statementList, JdbcConstants.MYSQL, parameters, sqlFormatOption);
        } catch (ClassCastException | ParserException ex) {
            LOG.warn("format error", ex);
            return sql;
        }
    }

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
        setParameter(parameterIndex, createParameterNull(sqlType));
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setParameter(parameterIndex, createParameter(Types.BOOLEAN, x));
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setParameter(parameterIndex, createParameter(Types.TINYINT, x));
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setParameter(parameterIndex, createParameter(Types.SMALLINT, x));
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        setParameter(parameterIndex, createParameter(x));
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        setParameter(parameterIndex, createParameter(x));
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setParameter(parameterIndex, createParameter(Types.FLOAT, x));
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setParameter(parameterIndex, createParameter(Types.DOUBLE, x));
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        setParameter(parameterIndex, createParameter(x));
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        setParameter(parameterIndex, createParameter(x));
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        setParameter(parameterIndex, createParameter(JdbcParameter.TYPE.BYTES, x));
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        setParameter(parameterIndex, createParameter(x));
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        setParameter(parameterIndex, createParameter(Types.TIME, x));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setParameter(parameterIndex, createParameter(x));
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setParameter(parameterIndex, createParameter(JdbcParameter.TYPE.AsciiInputStream, x, length));
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setParameter(parameterIndex, createParameter(JdbcParameter.TYPE.UnicodeStream, x, length));
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setParameter(parameterIndex, createParameter(JdbcParameter.TYPE.BinaryInputStream, x, length));
    }

    @Override
    public void clearParameters() throws SQLException {

    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setParameter(parameterIndex, createParameter(targetSqlType, x));
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        setObjectParameter(parameterIndex, x);
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
        setParameter(parameterIndex, createParameter(JdbcParameter.TYPE.CharacterInputStream, reader, length));
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        setParameter(parameterIndex, createParameter(Types.REF, x));
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        setParameter(parameterIndex, createParameter(Types.BLOB, x));
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        setParameter(parameterIndex, createParameter(Types.CLOB, x));
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        setParameter(parameterIndex, createParameter(Types.ARRAY, x));
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        setParameter(parameterIndex, createParameter(Types.DATE, x, cal));
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        setParameter(parameterIndex, createParameter(Types.TIME, x, cal));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        setParameter(parameterIndex, createParameter(Types.TIMESTAMP, x, cal));
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setParameter(parameterIndex, createParameterNull(sqlType));
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        setParameter(parameterIndex, createParameter(JdbcParameter.TYPE.URL, x));
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        setParameter(parameterIndex, createParameter(Types.ROWID, x));
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setParameter(parameterIndex, createParameter(Types.NVARCHAR, value));
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        setParameter(parameterIndex, createParameter(JdbcParameter.TYPE.NCharacterInputStream, value, length));
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        setParameter(parameterIndex, createParameter(Types.NCLOB, value));
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        setParameter(parameterIndex, createParameter(Types.CLOB, reader, length));
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        setParameter(parameterIndex, createParameter(Types.BLOB, inputStream, length));
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        setParameter(parameterIndex, createParameter(Types.NCLOB, reader, length));
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        setParameter(parameterIndex, createParameter(Types.SQLXML, xmlObject));
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setParameter(parameterIndex, createParameter(x, targetSqlType, scaleOrLength));
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        setParameter(parameterIndex, createParameter(JdbcParameter.TYPE.AsciiInputStream, x, length));
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        setParameter(parameterIndex, createParameter(JdbcParameter.TYPE.BinaryInputStream, x, length));
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        setParameter(parameterIndex, createParameter(JdbcParameter.TYPE.CharacterInputStream, reader, length));
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        setParameter(parameterIndex, createParameter(JdbcParameter.TYPE.AsciiInputStream, x));
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        setParameter(parameterIndex, createParameter(JdbcParameter.TYPE.BinaryInputStream, x));
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        setParameter(parameterIndex, createParameter(JdbcParameter.TYPE.CharacterInputStream, reader));
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        setParameter(parameterIndex, createParameter(JdbcParameter.TYPE.NCharacterInputStream, value));
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        setParameter(parameterIndex, createParameter(Types.CLOB, reader));
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        setParameter(parameterIndex, createParameter(Types.BLOB, inputStream));
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        setParameter(parameterIndex, createParameter(Types.NCLOB, reader));
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
        if (this.results != null) {
            this.results.close();
        }

        this.results = null;
        this.parameters = null;
        this.parametersSize = 0;
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
        return this.results;
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

    public void setResults(ResultSet results) {
        this.results = results;
    }

    private JdbcParameter createParameterNull(int sqlType) {
        return JdbcParameterNull.valueOf(sqlType);
    }

    private JdbcParameter createParameter(int sqlType, Object value) {
        if (value == null) {
            return JdbcParameterNull.valueOf(sqlType);
        }

        return new JdbcParameterImpl(sqlType, value);
    }

    private JdbcParameter createParameter(int x) {
        return JdbcParameterInt.valueOf(x);
    }

    private JdbcParameter createParameter(long x) {
        return JdbcParameterLong.valueOf(x);
    }

    private JdbcParameter createParameter(BigDecimal x) {
        if (x == null) {
            return JdbcParameterNull.DECIMAL;
        }

        return JdbcParameterDecimal.valueOf(x);
    }

    private JdbcParameter createParameter(String x) {
        if (x == null) {
            return JdbcParameterNull.VARCHAR;
        }

        if (x.length() == 0) {
            return JdbcParameterString.empty;
        }

        return new JdbcParameterString(x);
    }

    private JdbcParameter createParameter(java.util.Date x) {
        if (x == null) {
            return JdbcParameterNull.DATE;
        }

        return new JdbcParameterDate(x);
    }

    private JdbcParameter createParameter(int sqlType, Object value, long length) {
        if (value == null) {
            return JdbcParameterNull.valueOf(sqlType);
        }

        return new JdbcParameterImpl(sqlType, value, length);
    }

    private void setObjectParameter(int parameterIndex, Object x) {
        if (x == null) {
            setParameter(parameterIndex, createParameterNull(Types.OTHER));
            return;
        }

        Class<?> clazz = x.getClass();
        if (clazz == Byte.class) {
            setParameter(parameterIndex, createParameter(Types.TINYINT, x));
            return;
        }

        if (clazz == Short.class) {
            setParameter(parameterIndex, createParameter(Types.SMALLINT, x));
            return;
        }

        if (clazz == Integer.class) {
            setParameter(parameterIndex, createParameter((Integer) x));
            return;
        }

        if (clazz == Long.class) {
            setParameter(parameterIndex, createParameter((Long) x));
            return;
        }

        if (clazz == String.class) {
            setParameter(parameterIndex, createParameter((String) x));
            return;
        }

        if (clazz == BigDecimal.class) {
            setParameter(parameterIndex, createParameter((BigDecimal) x));
            return;
        }

        if (clazz == Float.class) {
            setParameter(parameterIndex, new JdbcParameterImpl(Types.FLOAT, x));
            return;
        }

        if (clazz == Double.class) {
            setParameter(parameterIndex, new JdbcParameterImpl(Types.DOUBLE, x));
            return;
        }

        if (clazz == java.sql.Date.class || clazz == java.util.Date.class) {
            setParameter(parameterIndex, createParameter((java.util.Date) x));
            return;
        }

        if (clazz == java.sql.Timestamp.class) {
            setParameter(parameterIndex, createParameter((java.sql.Timestamp) x));
            return;
        }

        if (clazz == java.sql.Time.class) {
            setParameter(parameterIndex, new JdbcParameterImpl(Types.TIME, x));
            return;
        }

        if (clazz == Boolean.class) {
            setParameter(parameterIndex, new JdbcParameterImpl(Types.BOOLEAN, x));
            return;
        }

        if (clazz == byte[].class) {
            setParameter(parameterIndex, new JdbcParameterImpl(JdbcParameter.TYPE.BYTES, x));
            return;
        }

        if (x instanceof InputStream) {
            setParameter(parameterIndex, new JdbcParameterImpl(JdbcParameter.TYPE.BinaryInputStream, x));
            return;
        }

        if (x instanceof Reader) {
            setParameter(parameterIndex, new JdbcParameterImpl(JdbcParameter.TYPE.CharacterInputStream, x));
            return;
        }

        if (x instanceof Clob) {
            setParameter(parameterIndex, new JdbcParameterImpl(Types.CLOB, x));
            return;
        }

        if (x instanceof NClob) {
            setParameter(parameterIndex, new JdbcParameterImpl(Types.NCLOB, x));
            return;
        }

        if (x instanceof Blob) {
            setParameter(parameterIndex, new JdbcParameterImpl(Types.BLOB, x));
            return;
        }

        setParameter(parameterIndex, createParameter(Types.OTHER, null));
    }

    private JdbcParameter createParameter(int sqlType, Object value, Calendar calendar) {
        if (value == null) {
            return JdbcParameterNull.valueOf(sqlType);
        }

        return new JdbcParameterImpl(sqlType, value, calendar);
    }

    private JdbcParameter createParameter(Object x, int sqlType, int scaleOrLength) {
        if (x == null) {
            return JdbcParameterNull.valueOf(sqlType);
        }

        return new JdbcParameterImpl(sqlType, x, -1, null, scaleOrLength);
    }

    private void setParameter(int jdbcIndex, JdbcParameter parameter) {
        int index = jdbcIndex - 1;

        if (jdbcIndex > parametersSize) {
            parametersSize = jdbcIndex;
        }
        if (parametersSize >= parameters.length) {
            int oldCapacity = parameters.length;
            int newCapacity = oldCapacity + (oldCapacity >> 1);
            if (newCapacity <= 4) {
                newCapacity = 4;
            }

            parameters = Arrays.copyOf(parameters, newCapacity);
        }
        parameters[index] = parameter;
    }
}
