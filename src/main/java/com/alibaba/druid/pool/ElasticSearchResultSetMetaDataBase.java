package com.alibaba.druid.pool;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.druid.util.jdbc.ResultSetMetaDataBase;

/**
 * Created by allwefantasy on 8/31/16.
 */
public class ElasticSearchResultSetMetaDataBase extends ResultSetMetaDataBase {
	
    private final List<ColumnMetaData> columns = new ArrayList<ColumnMetaData>();

    public ElasticSearchResultSetMetaDataBase(List<String> headers) {
        for(String column:headers){
            ColumnMetaData columnMetaData = new ColumnMetaData();
            columnMetaData.setColumnLabel(column);
            columnMetaData.setColumnName(column);
            columns.add(columnMetaData);
        }
    }
    
    @Override
    public List<ColumnMetaData> getColumns() {
        return columns;
    }

    @Override
    public int findColumn(String columnName) throws SQLException {
        ColumnMetaData column;
        for (int i = 0; i < columns.size(); ++i) {
            column = columns.get(i);
            if (column.getColumnName().equals(columnName)) {
                return i + 1;
            }
        }

        throw new SQLException("column '" + columnName + "' not found.");
    }
    
    @Override
    public int getColumnCount() throws SQLException {
        return columns.size();
    }

    @Override
    public ResultSetMetaDataBase.ColumnMetaData getColumn(int column) {
        return columns.get(column - 1);
    }
}
