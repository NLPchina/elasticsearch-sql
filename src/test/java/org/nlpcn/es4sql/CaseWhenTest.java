package org.nlpcn.es4sql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLFeatureNotSupportedException;

import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.SqlElasticRequestBuilder;

import com.google.common.io.Files;

public class CaseWhenTest {
	
    @Test
    public void query1() throws IOException, SqlParseException, NoSuchMethodException, IllegalAccessException, SQLFeatureNotSupportedException, InvocationTargetException {
        String expectedOutput = Files.toString(new File("src/test/resources/expectedOutput/casewhen_explain.json"), StandardCharsets.UTF_8).replaceAll("\r","");
        String result = explain(String.format("SELECT case when birth is null then '0' end test ,cust_code  FROM custom where birth between 19900101 and 19910101", TEST_INDEX));
        assertThat(result, equalTo(expectedOutput));
    }
    
    @Test
    public void query2() throws IOException, SqlParseException, NoSuchMethodException, IllegalAccessException, SQLFeatureNotSupportedException, InvocationTargetException {
        String expectedOutput = Files.toString(new File("src/test/resources/expectedOutput/casewhen_explain2.json"), StandardCharsets.UTF_8).replaceAll("\r","");
        String result = explain(String.format("SELECT case when birth > 19900101 and birth<19910101 or dat>1  and dat<1 then 'aa' when birth is null then '0' end test ,cust_code  FROM custom where birth between 19900101 and 19910101", TEST_INDEX));
        assertThat(result, equalTo(expectedOutput));
    }
    
    @Test
    public void query3() throws IOException, SqlParseException, NoSuchMethodException, IllegalAccessException, SQLFeatureNotSupportedException, InvocationTargetException {
        String expectedOutput = Files.toString(new File("src/test/resources/expectedOutput/casewhen_explain3.json"), StandardCharsets.UTF_8).replaceAll("\r","");
      String result = explain(String.format("SELECT case when (cust_subscribe_il-sub_time-fund_code-5150_last is not null or cust_subscribe_il-sub_time-fund_code-5151_last is not null or cust_subscribe_il-sub_time-fund_code-5152_last is not null) then '订阅' else '为订阅' end test,cust_code from custom", TEST_INDEX));
        assertThat(result, equalTo(expectedOutput));
    }
    
    @Test
    public void query4() throws IOException, SqlParseException, NoSuchMethodException, IllegalAccessException, SQLFeatureNotSupportedException, InvocationTargetException {
        String expectedOutput = Files.toString(new File("src/test/resources/expectedOutput/casewhen_explain4.json"), StandardCharsets.UTF_8).replaceAll("\r","");
        String result = explain(String.format("select case when channel_id=1 then '线上' else '线下' end channel_id_line,open_date,edu from custom/job where  (cust_type between 0 and 4 and priv_plac is null and non_trade is null) group by channel_id_line, terms('field'='open_date','missing'='-999999999999','alias'='open_date','size'=10000 ), terms('field'='edu','missing'='','alias'='edu','size'=10000 ) order by channel_id_line desc,aaa  limit  5000 ", TEST_INDEX));
        assertThat(result, equalTo(expectedOutput));
    }
    
    @Test
    public void query5() throws IOException, SqlParseException, NoSuchMethodException, IllegalAccessException, SQLFeatureNotSupportedException, InvocationTargetException {
        String expectedOutput = Files.toString(new File("src/test/resources/expectedOutput/casewhen_explain5.json"), StandardCharsets.UTF_8).replaceAll("\r","");
        String result = explain(String.format("select case when channel_id=1 then '线上' else '线下' end channel_id_line,open_date,edu from custom/job order by channel_id_line asc,aaa  limit  20 ", TEST_INDEX));
        
        assertThat(result, equalTo(expectedOutput));
    }

    private String explain(String sql) throws SQLFeatureNotSupportedException, SqlParseException, InvocationTargetException, NoSuchMethodException, IllegalAccessException, IOException {
		SearchDao searchDao = MainTestSuite.getSearchDao();
        SqlElasticRequestBuilder requestBuilder = searchDao.explain(sql).explain();
        return requestBuilder.explain();
	}
}
