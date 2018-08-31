package com.alibaba.druid.pool;

import com.alibaba.druid.util.jdbc.ResultSetBase;
import com.alibaba.druid.util.jdbc.ResultSetMetaDataBase;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by allwefantasy on 8/31/16.
 */
public class ElasticSearchResultSetMetaDataBase extends ResultSetMetaDataBase {
    private final List<ColumnMetaData> columns = super.getColumns();

    public ElasticSearchResultSetMetaDataBase(List<String> headers) {
        for(String column:headers){
            ColumnMetaData columnMetaData = new ColumnMetaData();
            columnMetaData.setColumnLabel(column);
            columnMetaData.setColumnName(column);
            columns.add(columnMetaData);
        }
    }
}
