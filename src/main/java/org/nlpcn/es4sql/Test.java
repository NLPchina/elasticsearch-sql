package org.nlpcn.es4sql;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by allwefantasy on 8/18/16.
 */
public class Test {
    public static String sqlToEsQuery(String sql) throws Exception {
        Map actions = new HashMap();
        Client client = new NodeClient(null, null, null, actions, null);
        SearchDao searchDao = new org.nlpcn.es4sql.SearchDao(client);
        try {
            return searchDao.explain(sql).explain().explain();
        } catch (Exception e) {
            throw e;
        }
    }

    public static void main(String[] args) throws Exception {
        String sql = "select floor(num,2) as num2 from twitter2 limit 10";
        System.out.println(sqlToEsQuery(sql));
    }
}
