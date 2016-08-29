package org.nlpcn.es4sql;


import com.google.common.collect.Maps;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.jdbc.ElasticsearchSchema;
import org.nlpcn.es4sql.jdbc.ElasticsearchSchemaFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by allwefantasy on 8/26/16.
 */
public class JDBCTests {
    @Test
    public void testJDBC() throws IOException, SqlParseException, NoSuchMethodException, IllegalAccessException, SQLException, InvocationTargetException, ClassNotFoundException {
        Class.forName("org.apache.calcite.jdbc.Driver");
        Properties info = new Properties();
        info.setProperty("lex", "JAVA");
        Connection connection = DriverManager.getConnection("jdbc:calcite://127.0.0.1:9200", info);
        CalciteConnection calciteConnection =
                connection.unwrap(CalciteConnection.class);

        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        Map<String, Integer> cordinals = new HashMap<>();
        cordinals.put("127.0.0.1", 9300);

        Map<String, String> userConfig = new HashMap<String, String>();

        rootSchema.add("cluster1", new ElasticsearchSchema(cordinals, userConfig, "twitter2"));


        Statement statement = calciteConnection.createStatement();
        ResultSet resultSet = statement.executeQuery(
                "select * from cluster1.twitter2  limit 10");
        while (resultSet.next()) {
            System.out.println(resultSet.getString(""));
        }

        resultSet.close();
        statement.close();
        connection.close();
    }

}


