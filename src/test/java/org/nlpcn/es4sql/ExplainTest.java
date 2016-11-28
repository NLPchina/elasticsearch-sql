package org.nlpcn.es4sql;

import com.google.common.io.Files;
import junit.framework.Assert;
import org.elasticsearch.action.ActionRequestBuilder;
import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.SqlElasticRequestBuilder;


import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLFeatureNotSupportedException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.nlpcn.es4sql.TestsConstants.DATE_FORMAT;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX;
import static org.hamcrest.Matchers.*;

public class ExplainTest {

	@Test
	public void searchSanity() throws IOException, SqlParseException, NoSuchMethodException, IllegalAccessException, SQLFeatureNotSupportedException, InvocationTargetException {
		String expectedOutput = Files.toString(new File("src/test/resources/expectedOutput/search_explain.json"), StandardCharsets.UTF_8).replaceAll("\r","");
		String result = explain(String.format("SELECT * FROM %s WHERE firstname LIKE 'A%%' AND age > 20 GROUP BY gender order by _score", TEST_INDEX));
		assertThat(result, equalTo(expectedOutput));
	}
	
    @Test
    public void aggregationQuery() throws IOException, SqlParseException, NoSuchMethodException, IllegalAccessException, SQLFeatureNotSupportedException, InvocationTargetException {
//        String expectedOutput = Files.toString(new File("src/test/resources/expectedOutput/aggregation_query_explain.json"), StandardCharsets.UTF_8).replaceAll("\r","");
      String result = explain(String.format("SELECT double(birth)/2 ,cust_code  FROM custom where birth between 19900101 and 19910101", TEST_INDEX));
//      String result = explain(String.format("SELECT concat(birth,aaa,bbbb) ,cust_code  FROM custom where birth between 19900101 and 19910101", TEST_INDEX));
//      String result = explain(String.format("SELECT length(toString(birth)) ,cust_code  FROM custom where birth between 19900101 and 19910101", TEST_INDEX));
//      String result = explain(String.format("SELECT floor((20161125-double(birth))/1000) ,cust_code  FROM custom where birth >0", TEST_INDEX));
//        String result = explain(String.format("SELECT substring(toString(open_date),0,1),count(cust_code)  FROM custom   group by bbb,terms('script'=substring(toString(open_date),0,1),'missing'='-999999999999','alias'='aaa') order by aa desc ", TEST_INDEX));
        
        System.out.println(result);
//        assertThat(result, equalTo(expectedOutput));
    }

	@Test
	public void searchSanityFilter() throws IOException, SqlParseException, NoSuchMethodException, IllegalAccessException, SQLFeatureNotSupportedException, InvocationTargetException {
		String expectedOutput = Files.toString(new File("src/test/resources/expectedOutput/search_explain_filter.json"), StandardCharsets.UTF_8).replaceAll("\r","");
		String result = explain(String.format("SELECT * FROM %s WHERE firstname LIKE 'A%%' AND age > 20 GROUP BY gender", TEST_INDEX));

		assertThat(result, equalTo(expectedOutput));
	}

	@Test
	public void deleteSanity() throws IOException, SqlParseException, NoSuchMethodException, IllegalAccessException, SQLFeatureNotSupportedException, InvocationTargetException {
		String expectedOutput = Files.toString(new File("src/test/resources/expectedOutput/delete_explain.json"), StandardCharsets.UTF_8).replaceAll("\r","");;
		String result = explain(String.format("DELETE FROM %s WHERE firstname LIKE 'A%%' AND age > 20", TEST_INDEX));

		assertThat(result, equalTo(expectedOutput));
	}

    @Test
    public void spatialFilterExplainTest() throws IOException, SqlParseException, NoSuchMethodException, IllegalAccessException, SQLFeatureNotSupportedException, InvocationTargetException {
        String expectedOutput = Files.toString(new File("src/test/resources/expectedOutput/search_spatial_explain.json"), StandardCharsets.UTF_8).replaceAll("\r","");;
        String result = explain(String.format("SELECT * FROM %s WHERE GEO_INTERSECTS(place,'POLYGON ((102 2, 103 2, 103 3, 102 3, 102 2))')", TEST_INDEX));
        assertThat(result, equalTo(expectedOutput));
    }

    private String explain(String sql) throws SQLFeatureNotSupportedException, SqlParseException, InvocationTargetException, NoSuchMethodException, IllegalAccessException, IOException {
		SearchDao searchDao = MainTestSuite.getSearchDao();
        SqlElasticRequestBuilder requestBuilder = searchDao.explain(sql).explain();
        return requestBuilder.explain();
	}
}
