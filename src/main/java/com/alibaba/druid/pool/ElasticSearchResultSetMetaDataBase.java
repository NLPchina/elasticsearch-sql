package com.alibaba.druid.pool;

import com.alibaba.druid.util.jdbc.ResultSetMetaDataBase;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by allwefantasy on 8/31/16.fy
 * modify by sishu.yss
 */
public class ElasticSearchResultSetMetaDataBase extends ResultSetMetaDataBase {

    protected final List<ColumnMetaData> columns = new ArrayList<ColumnMetaData>();

    public ElasticSearchResultSetMetaDataBase(List<String> headers) {
        for(String column:headers){
            ColumnMetaData columnMetaData = new ColumnMetaData();
            columnMetaData.setColumnLabel(column);
            columnMetaData.setColumnName(column);
            columns.add(columnMetaData);
        }
    }


    public List<ColumnMetaData> getColumns() {
        return columns;
    }

    public int findColumn(String columnName) throws SQLException {
        for (int i = 0; i < columns.size(); ++i) {
            ColumnMetaData column = columns.get(i);
            if (column.getColumnName().equals(columnName)) {
                return i + 1;
            }
        }

        throw new SQLException("column '" + columnName + "' not found.");
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            return (T) this;
        }
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        if (iface == null) {
            return false;
        }

        return iface.isAssignableFrom(this.getClass());

    }

    @Override
    public int getColumnCount() throws SQLException {
        return columns.size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return getColumn(column).isAutoIncrement();
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return getColumn(column).isCaseSensitive();
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return getColumn(column).isSearchable();
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return getColumn(column).isCurrency();
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return getColumn(column).getNullable();
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return getColumn(column).isSigned();
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return getColumn(column).getColumnDisplaySize();
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumn(column).getColumnLabel();
    }

    public ColumnMetaData getColumn(int column) {
        return columns.get(column - 1);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return getColumn(column).getColumnName();
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return getColumn(column).getSchemaName();
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return getColumn(column).getPrecision();
    }

    @Override
    public int getScale(int column) throws SQLException {
        return getColumn(column).getScale();
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return getColumn(column).getTableName();
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return getColumn(column).getCatalogName();
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return getColumn(column).getColumnType();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return getColumn(column).getColumnTypeName();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return getColumn(column).isReadOnly();
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return getColumn(column).isWritable();
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return getColumn(column).isDefinitelyWritable();
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return getColumn(column).getColumnClassName();
    }
}
