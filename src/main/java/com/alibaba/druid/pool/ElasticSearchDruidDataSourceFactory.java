package com.alibaba.druid.pool;

import org.elasticsearch.client.internal.Client;

import javax.sql.DataSource;
import java.util.Properties;

/**
 * Created by allwefantasy on 8/30/16.
 */
public class ElasticSearchDruidDataSourceFactory extends DruidDataSourceFactory {

    @Override
    protected DataSource createDataSourceInternal(Properties properties) throws Exception {
        throw new UnsupportedOperationException();
    }

    public static DataSource createDataSource(Client client) {
        return new ElasticSearchDruidDataSource(client);
    }
}
