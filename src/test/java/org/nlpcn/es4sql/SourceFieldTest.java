package org.nlpcn.es4sql;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.Assert;
import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.SqlElasticSearchRequestBuilder;

import java.io.IOException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Set;

import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_ACCOUNT;

public class SourceFieldTest {

	@Test
	public void includeTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		SearchHits response = query(String.format("SELECT include('*name','*ge'),include('b*'),include('*ddre*'),include('gender') FROM %s/account LIMIT 1000", TEST_INDEX_ACCOUNT));
		for (SearchHit hit : response.getHits()) {
			Set<String> keySet = hit.getSourceAsMap().keySet();
			for (String field : keySet) {
				Assert.assertTrue(field.endsWith("name") || field.endsWith("ge") || field.startsWith("b") || field.contains("ddre") || field.equals("gender"));
			}
		}

	}
	
	@Test
	public void excludeTest() throws SqlParseException, SQLFeatureNotSupportedException {

		SearchHits response = query(String.format("SELECT exclude('*name','*ge'),exclude('b*'),exclude('*ddre*'),exclude('gender') FROM %s/account LIMIT 1000", TEST_INDEX_ACCOUNT));

		for (SearchHit hit : response.getHits()) {
			Set<String> keySet = hit.getSourceAsMap().keySet();
			for (String field : keySet) {
				Assert.assertFalse(field.endsWith("name") || field.endsWith("ge") || field.startsWith("b") || field.contains("ddre") || field.equals("gender"));
			}
		}
	}
	
	@Test
	public void allTest() throws SqlParseException, SQLFeatureNotSupportedException {

		SearchHits response = query(String.format("SELECT exclude('*name','*ge'),include('b*'),exclude('*ddre*'),include('gender') FROM %s/account LIMIT 1000", TEST_INDEX_ACCOUNT));

		for (SearchHit hit : response.getHits()) {
			Set<String> keySet = hit.getSourceAsMap().keySet();
			for (String field : keySet) {
				Assert.assertFalse(field.endsWith("name") || field.endsWith("ge") ||  field.contains("ddre") );
				Assert.assertTrue(field.startsWith("b") || field.equals("gender"));
			}
		}
	}

	private SearchHits query(String query) throws SqlParseException, SQLFeatureNotSupportedException {
		SearchDao searchDao = MainTestSuite.getSearchDao();
		SqlElasticSearchRequestBuilder select = (SqlElasticSearchRequestBuilder) searchDao.explain(query).explain();
		return ((SearchResponse) select.get()).getHits();
	}

}
