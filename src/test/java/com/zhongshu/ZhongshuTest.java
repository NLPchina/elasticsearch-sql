package com.zhongshu;

import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.ESActionFactory;
import org.elasticsearch.client.Client;

import java.sql.SQLFeatureNotSupportedException;

/**
 * @author zhongshu
 * @since 2018/5/13 下午1:58
 */
public class ZhongshuTest {

    String sql = null;
    Client client = null;
    @Test
    public void testESActionFactoryCreate () throws SQLFeatureNotSupportedException, SqlParseException {

        sql = "select /*gg abc*/ a mya, b + 1 as myb, floor(c), case when d = 1 then 'hehe' when d <> 2 then 'haha' else 'gg' end as myd from TEST_TBL tbl";
        ESActionFactory.create(client, sql);
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
}
