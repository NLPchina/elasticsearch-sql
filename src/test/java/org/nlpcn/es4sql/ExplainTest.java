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
		String expectedOutput = Files.toString(new File("src/test/resources/expectedOutput/search_explain.json"), StandardCharsets.UTF_8);
		String result = explain(String.format("SELECT * FROM %s WHERE firstname LIKE 'A%%' AND age > 20 GROUP BY gender", TEST_INDEX));

		assertThat(result, equalTo(expectedOutput));
	}

	@Test
	public void deleteSanity() throws IOException, SqlParseException, NoSuchMethodException, IllegalAccessException, SQLFeatureNotSupportedException, InvocationTargetException {
		String expectedOutput = Files.toString(new File("src/test/resources/expectedOutput/delete_explain.json"), StandardCharsets.UTF_8);
		String result = explain(String.format("DELETE FROM %s WHERE firstname LIKE 'A%%' AND age > 20", TEST_INDEX));

		assertThat(result, equalTo(expectedOutput));
	}

    @Test
    public void spatialFilterExplainTest() throws IOException, SqlParseException, NoSuchMethodException, IllegalAccessException, SQLFeatureNotSupportedException, InvocationTargetException {
        String expectedOutput = Files.toString(new File("src/test/resources/expectedOutput/search_spatial_explain.json"), StandardCharsets.UTF_8);
        String result = explain(String.format("SELECT * FROM %s WHERE GEO_INTERSECTS(place,'POLYGON ((102 2, 103 2, 103 3, 102 3, 102 2))')", TEST_INDEX));
        assertThat(result, equalTo(expectedOutput));
    }

    private String explain(String sql) throws SQLFeatureNotSupportedException, SqlParseException, InvocationTargetException, NoSuchMethodException, IllegalAccessException, IOException {
		SearchDao searchDao = MainTestSuite.getSearchDao();
        SqlElasticRequestBuilder requestBuilder = searchDao.explain(sql);
        return requestBuilder.explain();
	}
}
