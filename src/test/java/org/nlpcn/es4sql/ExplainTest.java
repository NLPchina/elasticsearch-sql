package org.nlpcn.es4sql;

import com.alibaba.druid.support.json.JSONUtils;
import com.google.common.io.Files;
import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.SqlElasticRequestBuilder;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_LOCATION;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_NESTED_TYPE;

public class ExplainTest {

    @Test
    public void searchSanity() throws IOException, SqlParseException, NoSuchMethodException, IllegalAccessException, SQLFeatureNotSupportedException, InvocationTargetException {
        String expectedOutput = Files.toString(new File("src/test/resources/expectedOutput/search_explain.json"), StandardCharsets.UTF_8).replaceAll("\r","");
        String result = explain(String.format("SELECT * FROM %s WHERE firstname LIKE 'A%%' AND age > 20 GROUP BY gender order by _score", TEST_INDEX_ACCOUNT));

        assertThat(result.replaceAll("\\s+",""), equalTo(expectedOutput.replaceAll("\\s+","")));
    }
    
    @Test
    public void aggregationQuery() throws IOException, SqlParseException, NoSuchMethodException, IllegalAccessException, SQLFeatureNotSupportedException, InvocationTargetException {
        String expectedOutput = Files.toString(new File("src/test/resources/expectedOutput/aggregation_query_explain.json"), StandardCharsets.UTF_8).replaceAll("\r","");
        String result = explain(String.format("SELECT a, CASE WHEN gender='0' then 'aaa' else 'bbb'end a2345,count(c) FROM %s GROUP BY terms('field'='a','execution_hint'='global_ordinals'),a2345", TEST_INDEX_ACCOUNT));

        assertThat(result.replaceAll("\\s+",""), equalTo(expectedOutput.replaceAll("\\s+","")));
    }
    
    @Test
    public void explainScriptValue() throws IOException, SqlParseException, NoSuchMethodException, IllegalAccessException, SQLFeatureNotSupportedException, InvocationTargetException {
        String expectedOutput = Files.toString(new File("src/test/resources/expectedOutput/script_value.json"), StandardCharsets.UTF_8).replaceAll("\r","");
        String result = explain(String.format("SELECT  case when gender is null then 'aaa'  else gender  end  test , cust_code FROM %s", TEST_INDEX_ACCOUNT));

        assertThat(result.replaceAll("\\s+",""), equalTo(expectedOutput.replaceAll("\\s+","")));
    }
    
    @Test
    public void betweenScriptValue() throws IOException, SqlParseException, NoSuchMethodException, IllegalAccessException, SQLFeatureNotSupportedException, InvocationTargetException {
        String expectedOutput = Files.toString(new File("src/test/resources/expectedOutput/between_query.json"), StandardCharsets.UTF_8).replaceAll("\r","");
        String result = explain(String.format("SELECT  case when value between 100 and 200 then 'aaa'  else value  end  test , cust_code FROM %s", TEST_INDEX_ACCOUNT));

        assertThat(result.replaceAll("\\s+",""), equalTo(expectedOutput.replaceAll("\\s+","")));
    }

    @Test
    public void searchSanityFilter() throws IOException, SqlParseException, NoSuchMethodException, IllegalAccessException, SQLFeatureNotSupportedException, InvocationTargetException {
        String expectedOutput = Files.toString(new File("src/test/resources/expectedOutput/search_explain_filter.json"), StandardCharsets.UTF_8).replaceAll("\r","");
        String result = explain(String.format("SELECT * FROM %s WHERE firstname LIKE 'A%%' AND age > 20 GROUP BY gender", TEST_INDEX_ACCOUNT));

        assertThat(result.replaceAll("\\s+",""), equalTo(expectedOutput.replaceAll("\\s+","")));
    }

    @Test
    public void deleteSanity() throws IOException, SqlParseException, NoSuchMethodException, IllegalAccessException, SQLFeatureNotSupportedException, InvocationTargetException {
        String expectedOutput = Files.toString(new File("src/test/resources/expectedOutput/delete_explain.json"), StandardCharsets.UTF_8).replaceAll("\r","");;
        String result = explain(String.format("DELETE FROM %s WHERE firstname LIKE 'A%%' AND age > 20", TEST_INDEX_ACCOUNT));

        assertThat(result.replaceAll("\\s+",""), equalTo(expectedOutput.replaceAll("\\s+","")));
    }

    @Test
    public void spatialFilterExplainTest() throws IOException, SqlParseException, NoSuchMethodException, IllegalAccessException, SQLFeatureNotSupportedException, InvocationTargetException {
        String expectedOutput = Files.toString(new File("src/test/resources/expectedOutput/search_spatial_explain.json"), StandardCharsets.UTF_8).replaceAll("\r","");;
        String result = explain(String.format("SELECT * FROM %s WHERE GEO_INTERSECTS(place,'POLYGON ((102 2, 103 2, 103 3, 102 3, 102 2))')", TEST_INDEX_LOCATION));
        assertThat(result.replaceAll("\\s+",""), equalTo(expectedOutput.replaceAll("\\s+","")));
    }

    @Test
    public void orderByOnNestedFieldTest() throws Exception {
        String result = explain(String.format("SELECT * FROM %s ORDER BY NESTED('message.info','message')", TEST_INDEX_NESTED_TYPE));
        assertThat(result.replaceAll("\\s+", ""), equalTo("{\"from\":0,\"size\":1000,\"sort\":[{\"message.info\":{\"order\":\"asc\",\"nested\":{\"path\":\"message\"}}}]}"));
    }

    @Test
    public void multiMatchQuery() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
        String expectedOutput = Files.toString(new File("src/test/resources/expectedOutput/multi_match_query.json"), StandardCharsets.UTF_8).replaceAll("\r", "");
        String result = explain(String.format("SELECT * FROM %s WHERE q=multimatch(query='this is a test',fields='subject^3,message',analyzer='standard',type='best_fields',boost=1.0,slop=0,tie_breaker=0.3,operator='and')", TEST_INDEX_ACCOUNT));
        assertThat(result.replaceAll("\\s+", ""), equalTo(expectedOutput.replaceAll("\\s+", "")));
    }

    @Test
    public void termsIncludeExcludeExplainTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
        System.out.println(explain("SELECT * FROM index GROUP BY terms(field='correspond_brand_name',size='10',alias='correspond_brand_name',include='\".*sport.*\"',exclude='\"water_.*\"')"));
        System.out.println(explain("SELECT * FROM index GROUP BY terms(field='correspond_brand_name',size='10',alias='correspond_brand_name',include='[\"mazda\", \"honda\"]',exclude='[\"rover\", \"jensen\"]')"));
        System.out.println(explain("SELECT * FROM index GROUP BY terms(field='correspond_brand_name',size='10',alias='correspond_brand_name',include='{\"partition\":0,\"num_partitions\":20}')"));
    }

    @Test
    public void testSpanNearQueryExplain() throws SqlParseException, SQLFeatureNotSupportedException {
        System.out.println(explain("SELECT * FROM index WHERE q=span_near(boost=10.0,slop=12,in_order=false,clauses='[{\"span_term\":{\"field\":\"value1\"}},{\"span_term\":{\"field\":\"value2\"}},{\"span_term\":{\"field\":\"value3\"}}]')"));
    }

    @Test
    public void testCountDistinctExplain() throws SqlParseException, SQLFeatureNotSupportedException {
        System.out.println(explain("SELECT COUNT(DISTINCT sourceIP.keyword) AS size FROM dataflow WHERE startTime > 525757149439 AND startTime < 1525757449439 GROUP BY appName.keyword ORDER BY size DESC"));
    }

    @Test
    public void testStatsGroupsExplain() throws SqlParseException, SQLFeatureNotSupportedException {
        Map map = (Map) JSONUtils.parse(explain("SELECT /*! STATS(group1, group2) */ * FROM index"));
        assertThat(map.get("stats").toString(), equalTo("[group1, group2]"));
    }

    @Test
    public void testCastInWhereExplain() throws SqlParseException, SQLFeatureNotSupportedException {
        System.out.println(explain("select * from file1 where cast(offset as int) > 20"));
    }

    @Test
    public void testPreferenceExplain() throws SqlParseException, SQLFeatureNotSupportedException {
        System.out.println(explain("select /*! PREFERENCE(_shards:2,3|_local) */ * from myindex"));
    }

    @Test
    public void testCompareTwoFieldExplain() throws SqlParseException, SQLFeatureNotSupportedException {
        System.out.println(explain("select first_field,second_field from index-* where first_field=second_field and third_field=''"));
    }

    @Test
    public void testNotNestedExplain() throws SqlParseException, SQLFeatureNotSupportedException {
        System.out.println(explain("select * from test where not nested(\"tags\",tags.name=TERM(\"test\"))"));
    }

    @Test
    public void testMatchPhrasePrefixQueryExplain() throws SqlParseException, SQLFeatureNotSupportedException {
        System.out.println(explain("SELECT * FROM index WHERE q=match_phrase_prefix(query='this is a test',boost=10.0,slop=12)"));
    }

    @Test
    public void testDocvalueFieldQueryExplain() throws SqlParseException, SQLFeatureNotSupportedException {
        System.out.println(explain("SELECT docvalue('my_keyword_field') FROM index"));
        System.out.println(explain("SELECT docvalue('my_date_field','epoch_millis') FROM index"));
    }

    @Test
    public void testSignificantTextAggregationExplain() throws SqlParseException, SQLFeatureNotSupportedException {
        System.out.println(explain("SELECT * FROM index GROUP BY significant_text(field='my_field',alias='keywords',size=100,shard_size=100,min_doc_count=1)"));
    }

    @Test
    public void testSearchAfterHintQueryExplain() throws SqlParseException, SQLFeatureNotSupportedException {
        System.out.println(explain("select /*! SEARCH_AFTER(2021-05-20T05:30:04.832Z, 4294967298) */ * from index order by field_sort(['@timestamp'], missing='missing value', numeric_type='date_nanos', unmapped_type='long', format='strict_date_optional_time_nanos') desc, _shard_doc asc"));
    }

    @Test
    public void testDateFunctionQueryExplain() throws SqlParseException, SQLFeatureNotSupportedException {
        System.out.println(explain("select * from index where time >= date(date_add(date(now()), interval -0 day)) and time <= date_add(now(), interval -0 day)"));
    }

    @Test
    public void testFromUnixtimeFunctionQueryExplain() throws SqlParseException, SQLFeatureNotSupportedException {
        System.out.println(explain("select from_unixtime(unix_timestamp,'yyyy-MM-dd HH:mm:ss','+08:00') from index"));
    }

    private String explain(String sql) throws SQLFeatureNotSupportedException, SqlParseException {
        SearchDao searchDao = MainTestSuite.getSearchDao();
        SqlElasticRequestBuilder requestBuilder = searchDao.explain(sql).explain();
        return requestBuilder.explain();
    }
}
