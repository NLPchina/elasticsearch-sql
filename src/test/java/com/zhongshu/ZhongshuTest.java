package com.zhongshu;

import org.elasticsearch.plugin.nlpcn.executors.ActionRequestRestExecuterFactory;
import org.elasticsearch.plugin.nlpcn.executors.RestExecutor;
import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.ESActionFactory;
import org.elasticsearch.client.Client;
import org.nlpcn.es4sql.query.QueryAction;

import java.sql.SQLFeatureNotSupportedException;

/**
 * @author zhongshu
 * @since 2018/5/13 下午1:58
 * zhongshu-comment 以下所有sql例子都建议：不要给表加别名，字段名的别名不要和字段名一样
 */
public class ZhongshuTest {

    String sql = null;
    Client client = null;

    @Test
    public void testSelectStar() throws SQLFeatureNotSupportedException, SqlParseException {
        sql = "select a,case when c='1' then 'haha' when c='2' then 'book' else 'hbhb' end as gg from tbl_a group by a,gg"; // order by a asc,c desc,d asc limit 8,12";
//        sql = "select * from tbl_a group by a,b, case when c='1' then 'haha' when c='2' then 'book' else 'hbhb' end order by a asc,c desc,d asc limit 8,12";
//        sql = "select * from tbl_a group by a,b";
        QueryAction qa = ESActionFactory.create(client, sql);
        qa.explain();
    }

    @Test
    public void testESActionFactoryCreate () throws Exception {

//        sql = "select zs as zs, a mya, b + 1 as myb, floor(c), case when d = 1 then 'hehe' when d <> 2 then 'haha' else 'gg' end as myd from TEST_TBL";
        sql = "select a, floor(num) my_b, case when os = 'iOS' then 'hehe' else 'haha' end as my_os from t_zhongshu_test";
        QueryAction queryAction = ESActionFactory.create(client, sql);
        RestExecutor restExecutor = ActionRequestRestExecuterFactory.createExecutor(null);
        restExecutor.execute(client, null, queryAction, null);
        System.out.println();
    }

    @Test
    public void testHints() throws SQLFeatureNotSupportedException, SqlParseException {
        sql = "select /*! USE_SCROLL(10,120000) */ * FROM spark_es_table";
        ESActionFactory.create(client, sql);
    }

    /**
     * 报错，不通过，报错的地方：
     *  SqlParser类的parseSelect()方法里的解析from子句的那一行代码：select.getFrom().addAll(findFrom(query.getFrom()));
     * 不支持from子句的子查询
     * @throws SQLFeatureNotSupportedException
     * @throws SqlParseException
     */
    @Test
    public void testFromSubQuery() throws SQLFeatureNotSupportedException, SqlParseException {
        sql = "select dt,theo_inv from  (select \n" +
                "dt\n" +
                ",os\n" +
                ",click_num\n" +
                ",case when (ad_source ='中长尾' and os='iOS') and (dt='111' or sspid=998) then charge else '-' end as charge\n" +
                "from\n" +
                "t_md_xps2_all_inv_consume_report \n" +
                "where dt>='2018-05-01' and dt<='2018-05-10' \n" +
                ") tmp  where  1=1 and adslotid='13016'  order by dt asc , os desc";
//        ESActionFactory.create(client, sql);
        System.out.println(sql);
    }

    @Test
    public void testWhereSubQuery() throws SQLFeatureNotSupportedException, SqlParseException {
//        sql = "select goods_id,goods_name from goods\n" +
//                " where goods_id in (select max(goods_id) from goods group by cat_id);";
        sql = "select goods_id,goods_name from goods where goods_id = (select max(goods_id) from goods)";
        ESActionFactory.create(client, sql);
    }

    /**
     * 连接查询是走另外一条分支的，还是将默认那条分支走完再说
     * @throws SQLFeatureNotSupportedException
     * @throws SqlParseException
     */
    @Test
    public void testJoin() throws SQLFeatureNotSupportedException, SqlParseException {
        sql = "select * from " +
                "emp e inner join " +
                "org o on e.org_id = o.id ";
//                "left join " +
//                "TEST_TBL tbl on e.e_id = tbl.tbl_id";
        ESActionFactory.create(client, sql);
    }

    @Test
    public void testWhereClause() throws SQLFeatureNotSupportedException, SqlParseException {
//        sql = "select a,b,c as my_c from tbl where a = 1";
//        sql = "select a,b,c as my_c from tbl where a = 1 or b = 2 and c = 3";
        sql = "select a,b,c as my_c from tbl where a = 1 and b = 2 and c = 3";
//        sql = "select a,b,c as my_c from tbl where a = 1 or b = 2 and c = 3 and 1 > 1";
//        sql = "select a,b,c as my_c from tbl where a = 1 or b = 2 and (c = 3 or d = 4) and e>1";

        /*
        zhongshu-comment 这个sql例子举得不错，能比较清晰地呈现出它是如何解析where子句的，涵盖的情况比较全，最后的解析结果是：
        OR a = 1 Condition
        OR b = 2 and (c = 3 or d = 4) Where
            AND b = 2 Condition
            AND (c = 3 or d = 4) Where
                OR c = 3 Condition
                OR d = 4 Condition
        OR e > 1 Condition

        归纳总结：
            1、最小单元的条件就是一个Condition对象，例如：a=1、e>1这些
            2、如果组合了多个Condition的话，那就是一个Where对象，例如：b = 2 and (c = 3 or d = 4)
                要将Where对象进行拆分，拆成最细的Condition对象：b=2、c=3、d=4
            3、Condition有优先级之分And就被分在一块
                根据or去切分，然后and的聚在一块，例如下语句就分成3块：
                a = 1
                b = 2 and (c = 3 or d = 4)
                e > 1
         */
//        sql = "select a,b,c as my_c from tbl where a = 1 or b = 2 and (c = 3 or d = 4) or e > 1";
        QueryAction qa = ESActionFactory.create(client, sql);
        qa.explain();

    }

    @Test
    public void testStr () {
        String a = "abc";
        String b = "abc";
        System.out.println(a == b);
        System.out.println(a != b);
    }
}
