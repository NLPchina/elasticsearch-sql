package com.alibaba.druid.pool;

import javax.sql.DataSource;
import java.util.Map;
import java.util.Properties;

/**
 * Created by allwefantasy on 8/30/16.
 */
public class ElasticSearchDruidDataSourceFactory extends DruidDataSourceFactory {

    @Override
    protected DataSource createDataSourceInternal(Properties properties) throws Exception {
        DruidDataSource dataSource = new ElasticSearchDruidDataSource();
        config(dataSource, properties);
        return dataSource;
    }

    @SuppressWarnings("rawtypes")
    public static DataSource createDataSource(Properties properties) throws Exception {
        return createDataSource((Map) properties);
    }

    @SuppressWarnings("rawtypes")
    public static DataSource createDataSource(Map properties) throws Exception {
        DruidDataSource dataSource = new ElasticSearchDruidDataSource();
        config(dataSource, properties);
        return dataSource;
    }
}
