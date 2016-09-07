package org.nlpcn.es4sql;


import com.alibaba.druid.pool.DruidDataSource;

import com.alibaba.druid.pool.ElasticSearchDruidDataSourceFactory;
import org.junit.Test;
import java.sql.*;
import java.util.Properties;

/**
 * Created by allwefantasy on 8/26/16.
 */
public class JDBCTests {
    @Test
    public void testJDBC() throws Exception {
        Properties properties = new Properties();
        properties.put("url", "jdbc:elasticsearch://127.0.0.1:9300/twitter2");
        DruidDataSource dds = (DruidDataSource) ElasticSearchDruidDataSourceFactory.createDataSource(properties);
        dds.setInitialSize(1);
        Connection connection = dds.getConnection();
        PreparedStatement ps = connection.prepareStatement("SELECT split(trim(concat_ws('dd',newtype,num_d)),'dd')[0] as nt from  twitter2");
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            System.out.println(resultSet.getString("nt"));
        }

        ps.close();
        connection.close();
        dds.close();
    }

}


