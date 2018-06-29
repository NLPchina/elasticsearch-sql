package com.zhongshu;

import org.elasticsearch.plugin.nlpcn.executors.ActionRequestRestExecuterFactory;
import org.elasticsearch.plugin.nlpcn.executors.RestExecutor;
import org.junit.After;
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

    @After
    public void execute() throws SQLFeatureNotSupportedException, SqlParseException {
        QueryAction qa = ESActionFactory.create(client, sql);
        qa.explain();
    }

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
    public void testGroupBy() {
        sql = "select " +
                "count(*), " +
                "gg, " +
                "a as my_a, " +
                "floor(b) as my_b , " +
                "count(c) as my_count, " +
                "sum(d), " +
                "case when e='1' then 'hehe' when e='2' then 'haha' else 'book' end as my_e " +
                "from tbl " +
                "group by my_a, my_b";
        System.out.println(sql);
    }

    @Test
    public void testStr () {
        String a = "abc";
        String b = "abc";
        System.out.println(a == b);
        System.out.println(a != b);
    }

    @Test
    public void testOrderByCaseWhen() throws SQLFeatureNotSupportedException, SqlParseException {
        //zhongshu-comment 随便一个例子
        //        sql = "SELECT dt, os, appid\n" +
//                "FROM t_od_xps2_app_channel_cube_report\n" +
//                "WHERE dt >= '2018-05-29'\n" +
//                "\tAND dt <= '2018-05-29'\n" +
//                "ORDER BY CASE WHEN accounttype = '1' THEN effect_inv ELSE theo_inv END";

        //zhongshu-comment 松哥给的线上错误例子：在order by子句中直接使用select case when的别名排序
//        sql = "select  dt, os,  appid, case when accounttype='0' then '品牌' when  accounttype='1' then '中长尾'  else accounttype end as accounttype, channel_no, adslotid, adposition_name, dau, case when accounttype='1' then effect_inv else theo_inv end as theo_inv, v_num , av_num , click_num , case when accounttype='1' then charge else '-' end as charge, v_rate, av_rate, ctr1, ctr2, case when accounttype='1' then ecpm1 else '-' end ecpm1, case when accounttype='1' then ecpm2 else '-' end ecpm2, case when accounttype='1' then acp else '-' end acp from t_od_xps2_app_channel_cube_report  where dt>='2018-05-29' and dt<='2018-05-29'   and   1=1  and accounttype not in ('all','全部') and os = '全部' and appid not in ('all','全部') and adslotid in ('全部') and channel_no = '全部'  order by dt desc,theo_inv desc limit 0, 10";
//        sql = "select dt, os, appid, case when accounttype='0' then '品牌' when  accounttype='1' then '中长尾'  else accounttype end as accounttype, channel_no, adslotid, adposition_name, dau, case when accounttype='1' then effect_inv else theo_inv end as theo_inv, v_num , av_num , click_num , case when accounttype='1' then charge else '-' end as charge, case when v_rate='' then '0.00' else v_rate end as v_rate, case when av_rate='' then '0.00' else av_rate end as av_rate, case when ctr1='' then '0.00' else ctr1 end as ctr1, case when ctr2='' then '0.00' else ctr2 end as ctr2, case when ecpm1='' then '0.00' else ecpm1 end ecpm1, case when ecpm2='' then '0.00' else ecpm2 end ecpm2, case when acp='' then '0.00' else acp end acp, case when arpu='' then '0.00' else arpu end arpu from t_od_xps2_app_channel_cube_report where dt>='2018-05-27' and dt<='2018-06-03'  and   1=1  and accounttype = '全部' and os = '全部' and appid = '全部' and adslotid in ('全部') and channel_no = '全部'  order by dt desc,theo_inv desc limit 0, 10";
//        sql = "select dt, os, appid, case when accounttype='0' then '品牌' when  accounttype='1' then '中长尾'  else accounttype end as accounttype, channel_no, adslotid, adposition_name, dau, case when accounttype='1' then effect_inv else theo_inv end as theo_inv, v_num , av_num , click_num , case when accounttype='1' then charge else '-' end as charge, case when v_rate='' then '0.00' else v_rate end as v_rate, case when av_rate='' then '0.00' else av_rate end as av_rate, case when ctr1='' then '0.00' else ctr1 end as ctr1, case when ctr2='' then '0.00' else ctr2 end as ctr2, case when ecpm1='' then '0.00' else ecpm1 end ecpm1, case when ecpm2='' then '0.00' else ecpm2 end ecpm2, case when acp='' then '0.00' else acp end acp, case when arpu='' then '0.00' else arpu end arpu from t_od_xps2_app_channel_cube_report where dt>='2018-05-29' and dt<='2018-05-29'  and   1=1  and accounttype not in ('all','全部') and os not in ('all','全部') and appid not in ('all','全部') and adslotid in ('全部') and channel_no = '全部'  order by dt desc,theo_inv desc limit 0, 10";

        //zhongshu-comment 修正了松哥给的例子：在order by子句中使用了case when子句做排序
//        sql = "select  dt, os,  appid, case when accounttype='0' then '品牌' when  accounttype='1' then '中长尾'  else accounttype end as accounttype, channel_no, adslotid, adposition_name, dau, case when accounttype='1' then effect_inv else theo_inv end as theo_inv, v_num , av_num , click_num , case when accounttype='1' then charge else '-' end as charge, v_rate, av_rate, ctr1, ctr2, case when accounttype='1' then ecpm1 else '-' end ecpm1, case when accounttype='1' then ecpm2 else '-' end ecpm2, case when accounttype='1' then acp else '-' end acp from t_od_xps2_app_channel_cube_report  where dt>='2018-05-29' and dt<='2018-05-29'   and   1=1  and accounttype not in ('all','全部') and os = '全部' and appid not in ('all','全部') and adslotid in ('全部') and channel_no = '全部'  order by dt desc,case when accounttype='1' then effect_inv else theo_inv end desc limit 0, 10";
//        sql = "select dt, os, appid, case when accounttype='0' then '品牌' when  accounttype='1' then '中长尾'  else accounttype end as accounttype, channel_no, adslotid, adposition_name, dau, case when accounttype='1' then effect_inv else theo_inv end as theo_inv, v_num , av_num , click_num , case when accounttype='1' then charge else '-' end as charge, case when v_rate='' then '0.00' else v_rate end as v_rate, case when av_rate='' then '0.00' else av_rate end as av_rate, case when ctr1='' then '0.00' else ctr1 end as ctr1, case when ctr2='' then '0.00' else ctr2 end as ctr2, case when ecpm1='' then '0.00' else ecpm1 end ecpm1, case when ecpm2='' then '0.00' else ecpm2 end ecpm2, case when acp='' then '0.00' else acp end acp, case when arpu='' then '0.00' else arpu end arpu from t_od_xps2_app_channel_cube_report where dt>='2018-05-27' and dt<='2018-06-03'  and   1=1  and accounttype = '全部' and os = '全部' and appid = '全部' and adslotid in ('全部') and channel_no = '全部'  order by dt desc,case when accounttype='1' then effect_inv else theo_inv end desc limit 0, 10";
        sql = "select dt, os, appid, case when accounttype='0' then '品牌' when  accounttype='1' then '中长尾'  else accounttype end as accounttype, channel_no, adslotid, adposition_name, dau, case when accounttype='1' then effect_inv else theo_inv end as theo_inv, v_num , av_num , click_num , case when accounttype='1' then charge else '-' end as charge, case when v_rate='' then '0.00' else v_rate end as v_rate, case when av_rate='' then '0.00' else av_rate end as av_rate, case when ctr1='' then '0.00' else ctr1 end as ctr1, case when ctr2='' then '0.00' else ctr2 end as ctr2, case when ecpm1='' then '0.00' else ecpm1 end ecpm1, case when ecpm2='' then '0.00' else ecpm2 end ecpm2, case when acp='' then '0.00' else acp end acp, case when arpu='' then '0.00' else arpu end arpu from t_od_xps2_app_channel_cube_report where dt>='2018-05-29' and dt<='2018-05-29'  and   1=1  and accounttype not in ('all','全部') and os not in ('all','全部') and appid not in ('all','全部') and adslotid in ('全部') and channel_no = '全部'  order by dt desc,case when accounttype='1' then effect_inv else theo_inv end desc limit 0, 10";
        QueryAction qa = ESActionFactory.create(client, sql);
        qa.explain();
    }

    @Test
    public void testInJudge() throws SqlParseException, SQLFeatureNotSupportedException {
//        sql = "select dt, case when a in ('1', '2', '3') then 'hehe' when b=2 then 'haha' when c not in (7,8,9) then 'book' else 'gg' end as a from tbl";
        sql = "select dt,case when os in ('Android', 'iOS') then 'hello' else 'world' end abc from t_zhongshu_test";
        QueryAction qa = ESActionFactory.create(client, sql);
        qa.explain();
    }

    @Test
    public void testRound() throws SqlParseException, SQLFeatureNotSupportedException {
        sql = "SELECT round(av_num/v_num,4) as av_rate1 FROM t_md_xps2_all_inv_consume_report WHERE dt = '2018-06-14' limit 100";
        QueryAction qa = ESActionFactory.create(client, sql);
        qa.explain();
    }

    @Test
    public void testStringOrderBy() throws SQLFeatureNotSupportedException, SqlParseException {
        sql = "SELECT dt\n" +
                "\t, CASE\n" +
                "\t\tWHEN data_source = '0' THEN '新品算'\n" +
                "\t\tWHEN data_source = '1' THEN '汇算'\n" +
                "\t\tELSE data_source\n" +
                "\tEND AS data_source, os, platform_id, ssp_id, adslotid\n" +
                "\t, adposition_name, ad_source, appid, appdelaytrack, bidtype\n" +
                "\t, stock, v_num, av_num, click_num\n" +
                "\t, CASE\n" +
                "\t\tWHEN ad_source = '中长尾' THEN charge\n" +
                "\t\tELSE '-'\n" +
                "\tEND AS charge, av_stock, v_rate, av_rate, ctr1\n" +
                "\t, ctr2\n" +
                "\t, CASE\n" +
                "\t\tWHEN ad_source = '中长尾' THEN ecpm1\n" +
                "\t\tELSE '-'\n" +
                "\tEND AS ecpm1\n" +
                "\t, CASE\n" +
                "\t\tWHEN ad_source = '中长尾' THEN ecpm2\n" +
                "\t\tELSE '-'\n" +
                "\tEND AS ecpm2\n" +
                "\t, CASE\n" +
                "\t\tWHEN ad_source = '中长尾' THEN acp\n" +
                "\t\tELSE '-'\n" +
                "\tEND AS acp\n" +
                "FROM t_md_xps2_all_inv_consume_report_v2\n" +
                "WHERE dt >= '2018-05-20'\n" +
                "AND dt <= '2018-06-19'\n" +
                "AND 1 = 1\n" +
                "AND data_source NOT IN ('all', '全部')\n" +
                "AND ad_source = '全部'\n" +
                "AND os = '全部'\n" +
                "AND appdelaytrack = '全部'\n" +
                "AND platform_id = '全部'\n" +
                "AND appid = '全部'\n" +
                "AND ssp_id = '全部'\n" +
                "AND bidtype = '全部'\n" +
                "AND adslotid IN ('全部')\n" +
                "ORDER BY dt DESC, CASE\n" +
                "\tWHEN data_source = '0' THEN '新品算'\n" +
                "\tWHEN data_source = '1' THEN '汇算'\n" +
                "\tELSE data_source\n" +
                "END ASC";
        QueryAction qa = ESActionFactory.create(client, sql);
        qa.explain();
    }

    @Test
    public void testIf() throws SqlParseException, SQLFeatureNotSupportedException{
        sql = "SELECT IF(t.a=1,'1','2') from t";
        QueryAction qa = ESActionFactory.create(client, sql);
        qa.explain();
    }

    @Test
    public void testGroupBySize() throws SQLFeatureNotSupportedException, SqlParseException {
        sql = "SELECT \n" +
                "aid,\n" +
                "appid,\n" +
                "accounttype,\n" +
                "(case when bidtype='1' then 'CPD' when bidtype='2' then 'CPM' when bidtype='3' then 'CPC' when bidtype='4' then 'DCPM' else '其它' end ) as bidtype,\n" +
                "sum(count_v) as count_v,\n" +
                "sum(count_av) as count_av,\n" +
                "sum(count_click) as count_click,\n" +
                "sum(sum_charge) as sum_charge \n" +
                "FROM t_fact_tracking_charge where 1=1 and appid in ('news','newssdk','wapnews','pcnews','tv','h5tv','pctv','union','wapunion','squirrel')\n" +
                "and dt=20180628 group by aid,appid order by count_v desc";
        QueryAction qa = ESActionFactory.create(client, sql);
        qa.explain();
    }
}
