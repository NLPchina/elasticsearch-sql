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
        //String sql = "SELECT u as u2,count(distinct(mid)) as count FROM panda_quality where ty='buffer' and day='20160816' and tm>1471312800.00 and tm<1471313100.00 and domain='http://pl10.live.panda.tv' group by u  order by count desc limit 5000";
        String sql = "SELECT sum(num) as num2,newtype as nt  from  twitter2 group by nt  order by num2 ";
        //System.out.println(sqlToEsQuery(sql));

        //sql = "SELECT sum(num_d) as num2,split(newtype,',') as nt  from  twitter2 group by nt  order by num2 ";
        sql = "SELECT newtype as nt  from  twitter2  ";

        //sql = "SELECT sum(num_d) as num2,floor(num) as nt  from  twitter2 group by floor(num),newtype  order by num2 ";

        sql = "SELECT split('newtype','b') as nt,sum(num_d) as num2   from  twitter2 group by nt ";
        System.out.println(sqlToEsQuery(sql));
    }
}
