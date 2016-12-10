package org.nlpcn.es4sql;


import junit.framework.Assert;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;
import sun.applet.Main;

import java.io.IOException;
import java.sql.SQLFeatureNotSupportedException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX;

public class DeleteTest {

	@Before
	public void loadTempData() throws Exception {
		MainTestSuite.loadBulk("src/test/resources/accounts_temp.json");
	}

	@After
	public void deleteTempData() throws Exception {
        //todo: find a way to delete only specific type
        //MainTestSuite.deleteQuery(TEST_INDEX, "account_temp");
	}


	@Test
	public void deleteAllTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		delete(String.format("DELETE FROM %s/account_temp", TEST_INDEX));

		// Assert no results exist for this type.
		SearchRequestBuilder request = MainTestSuite.getClient().prepareSearch(TEST_INDEX);
		request.setTypes("account_temp");
		SearchResponse response = request.setQuery(QueryBuilders.matchAllQuery()).get();
		assertThat(response.getHits().getTotalHits(), equalTo(0L));
	}


	@Test
	public void deleteWithConditionTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		delete(String.format("DELETE FROM %s/phrase WHERE phrase = 'quick fox here' ", TEST_INDEX));
		// Assert no results exist for this type.
		SearchRequestBuilder request = MainTestSuite.getClient().prepareSearch(TEST_INDEX);
		request.setTypes("phrase");
		SearchResponse response = request.setQuery(QueryBuilders.matchAllQuery()).get();
		assertThat(response.getHits().getTotalHits(), equalTo(3L));
	}


	private void delete(String deleteStatement) throws SqlParseException, SQLFeatureNotSupportedException {
		SearchDao searchDao = MainTestSuite.getSearchDao();
		searchDao.explain(deleteStatement).explain().get();
        searchDao.getClient().admin().indices().prepareRefresh(TEST_INDEX).get();
	}
}
