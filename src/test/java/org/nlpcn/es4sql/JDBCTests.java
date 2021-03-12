package org.nlpcn.es4sql;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.ElasticSearchDruidDataSourceFactory;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_CONNECTIONPROPERTIES;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_URL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Created by allwefantasy on 8/26/16.
 */
public class JDBCTests {
    @Test
    public void testJDBC() throws Exception {
        Properties properties = new Properties();
        properties.put(PROP_URL, "jdbc:elasticsearch://127.0.0.1:9300/" + TestsConstants.TEST_INDEX_ACCOUNT);
        properties.put(PROP_CONNECTIONPROPERTIES, "client.transport.ignore_cluster_name=true");
        DruidDataSource dds = (DruidDataSource) ElasticSearchDruidDataSourceFactory.createDataSource(properties);
        Connection connection = dds.getConnection();
        PreparedStatement ps = connection.prepareStatement("SELECT /*! USE_SCROLL*/ gender,docvalue(gender.keyword),lastname,age,_scroll_id from  " + TestsConstants.TEST_INDEX_ACCOUNT + " where lastname='Heath'");
        ResultSet resultSet = ps.executeQuery();

        ResultSetMetaData metaData = resultSet.getMetaData();
        assertThat(metaData.getColumnName(1), equalTo("gender"));
        assertThat(metaData.getColumnName(2), equalTo("gender.keyword"));
        assertThat(metaData.getColumnName(3), equalTo("lastname"));
        assertThat(metaData.getColumnName(4), equalTo("age"));

        List<String> result = new ArrayList<String>();
        String scrollId = null;
        while (resultSet.next()) {
            scrollId = resultSet.getString("_scroll_id");
            result.add(resultSet.getString("lastname") + "," + resultSet.getInt("age") + "," + resultSet.getString("gender") + "," + resultSet.getString("gender.keyword"));
        }

        ps.close();
        connection.close();
        dds.close();

        Assert.assertEquals(1, result.size());
        Assert.assertEquals("Heath,39,F,F", result.get(0));
        Assert.assertFalse(Matchers.isEmptyOrNullString().matches(scrollId));
    }

    @Test
    public void testJDBCWithParameter() throws Exception {
        Properties properties = new Properties();
        properties.put(PROP_URL, "jdbc:elasticsearch://127.0.0.1:9300/" + TestsConstants.TEST_INDEX_ACCOUNT);
        properties.put(PROP_CONNECTIONPROPERTIES, "client.transport.ignore_cluster_name=true");
        try (DruidDataSource dds = (DruidDataSource) ElasticSearchDruidDataSourceFactory.createDataSource(properties);
             Connection connection = dds.getConnection();
             PreparedStatement ps = connection.prepareStatement("SELECT gender,lastname,age from " + TestsConstants.TEST_INDEX_ACCOUNT + " where lastname=?")) {
            // set parameter
            ps.setString(1, "Heath");
            ResultSet resultSet = ps.executeQuery();

            ResultSetMetaData metaData = resultSet.getMetaData();
            assertThat(metaData.getColumnName(1), equalTo("gender"));
            assertThat(metaData.getColumnName(2), equalTo("lastname"));
            assertThat(metaData.getColumnName(3), equalTo("age"));

            List<String> result = new ArrayList<>();
            while (resultSet.next()) {
                result.add(resultSet.getString("lastname") + "," + resultSet.getInt("age") + "," + resultSet.getString("gender"));
            }

            Assert.assertEquals(1, result.size());
            Assert.assertEquals("Heath,39,F", result.get(0));
        }
    }
}
