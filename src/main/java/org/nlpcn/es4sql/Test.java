package org.nlpcn.es4sql;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by allwefantasy on 8/18/16.
 */
public class Test {
    public static String sqlToEsQuery(String sql) throws Exception {
        Map actions = new HashMap();
        Settings settings = Settings.builder().build();
//        Client client = new NodeClient(settings, null, null, actions);
//        Settings.builder()
//                .put(ThreadContext.PREFIX + ".key1", "val1")
//                .put(ThreadContext.PREFIX + ".key2", "val 2")
//                .build();

        ThreadPool threadPool = new ThreadPool(settings);
        Client client = new NodeClient(settings, threadPool);
        SearchDao searchDao = new org.nlpcn.es4sql.SearchDao(client);
        try {
            return searchDao.explain(sql).explain().explain();
        } catch (Exception e) {
            throw e;
        }
    }

    public static void main(String[] args) throws Exception {
        String sql = "SELECT u as u2,count(distinct(mid)) as count FROM panda_quality where ty='buffer' and day='20160816' and tm>1471312800.00 and tm<1471313100.00 and domain='http://pl10.live.panda.tv' group by u  order by count desc limit 5000";
//        sql = "SELECT sum(num) as num2,newtype as nt  from  twitter2 group by nt  order by num2 ";
//        System.out.println("sql" + sql + ":\n" + sqlToEsQuery(sql));
//
//        sql = "SELECT sum(num_d) as num2,split(newtype,',') as nt  from  twitter2 group by nt  order by num2 ";
//
//        System.out.println("sql" + sql + ":\n" + sqlToEsQuery(sql));
//
//        sql = "SELECT newtype as nt  from  twitter2  ";
//
//        System.out.println("sql" + sql + ":\n----------\n" + sqlToEsQuery(sql));
//
//        sql = "SELECT sum(num_d) as num2,floor(num) as nt  from  twitter2 group by floor(num),newtype  order by num2 ";
//
//        System.out.println("sql" + sql + ":\n----------\n" + sqlToEsQuery(sql));
//
//        sql = "SELECT split('newtype','b')[1] as nt,sum(num_d) as num2   from  twitter2 group by nt ";
//
//        System.out.println("sql" + sql + ":\n----------\n" + sqlToEsQuery(sql));
//
//        sql = "SELECT concat_ws('dd','newtype','num_d') as num2   from  twitter2";
//        System.out.println("sql" + sql + ":\n----------\n" + sqlToEsQuery(sql));
//
//        sql = "SELECT split(split('newtype','c')[0],'b')[0] as num2   from  twitter2";
//        System.out.println("sql" + sql + ":\n----------\n" + sqlToEsQuery(sql));
//
//        sql = "SELECT floor(split(substring('newtype',0,3),'c')[0]) as num2   from  twitter2";
//        System.out.println("sql" + sql + ":\n----------\n" + sqlToEsQuery(sql));
//
//        sql = "SELECT split(substring('newtype',0,3),'c')[0] as nt,num_d   from  twitter2 group by nt";
//        System.out.println("sql" + sql + ":\n----------\n" + sqlToEsQuery(sql));
//
//        sql = "SELECT floor(num_d) as nt from  twitter2 ";
//        System.out.println("sql" + sql + ":\n----------\n" + sqlToEsQuery(sql));
//
//        sql = "SELECT trim(newtype) as nt from  twitter2 ";
//        System.out.println("sql" + sql + ":\n----------\n" + sqlToEsQuery(sql));
//
//        sql = "SELECT trim(concat_ws('dd',newtype,num_d)) as nt from  twitter2 ";
//        System.out.println("sql" + sql + ":\n----------\n" + sqlToEsQuery(sql));
//
//
//        sql = "SELECT split(trim(concat_ws('dd',newtype,num_d)),'dd')[0] as nt from  twitter2 ";
//        System.out.println("sql" + sql + ":\n----------\n" + sqlToEsQuery(sql));

        sql = "SELECT floor(" +
                "floor(substring(newtype,0,14)/100)/5)*5 as key," +
                "count(distinct(num)) cvalue FROM twitter2 " +
                "group by key ";
        String TEST_INDEX = "elasticsearch-sql_test_index";

        sql =  "select * from xxx/locs where 'a' = 'b' and a > 1";

        System.out.println("sql" + sql + ":\n----------\n" + sqlToEsQuery(sql));


    }
}
