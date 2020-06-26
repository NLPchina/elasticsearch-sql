package org.nlpcn.es4sql;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;

import java.sql.SQLFeatureNotSupportedException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_ACCOUNT_TEMP;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_PHRASE;

public class DeleteTest {

	@Before
	public void loadTempData() throws Exception {
		MainTestSuite.loadBulk("src/test/resources/accounts_temp.json", TEST_INDEX_ACCOUNT_TEMP);
        MainTestSuite.getSearchDao().getClient().admin().indices().prepareRefresh(TEST_INDEX_ACCOUNT_TEMP).get();
	}

	@After
	public void deleteTempData() throws Exception {
        //todo: find a way to delete only specific type
        //MainTestSuite.deleteQuery(TEST_INDEX, "account_temp");
	}


	@Test
	public void deleteAllTest() throws SqlParseException, SQLFeatureNotSupportedException {
		delete(String.format("DELETE FROM %s/temp_account", TEST_INDEX_ACCOUNT_TEMP), TEST_INDEX_ACCOUNT_TEMP);

		// Assert no results exist for this type.
		SearchRequestBuilder request = MainTestSuite.getClient().prepareSearch(TEST_INDEX_ACCOUNT_TEMP);
		request.setTypes("temp_account");
		SearchResponse response = request.setQuery(QueryBuilders.matchAllQuery()).get();
		assertThat(response.getHits().getTotalHits().value, equalTo(0L));
	}


	@Test
	public void deleteWithConditionTest() throws SqlParseException, SQLFeatureNotSupportedException {
		delete(String.format("DELETE FROM %s/phrase WHERE phrase = 'quick fox here' ", TEST_INDEX_PHRASE), TEST_INDEX_PHRASE);
		// Assert no results exist for this type.
		SearchRequestBuilder request = MainTestSuite.getClient().prepareSearch(TEST_INDEX_PHRASE);
		request.setTypes("phrase");
		SearchResponse response = request.setQuery(QueryBuilders.matchAllQuery()).get();
		assertThat(response.getHits().getTotalHits().value, equalTo(5L));
	}


	private void delete(String deleteStatement, String index) throws SqlParseException, SQLFeatureNotSupportedException {
		SearchDao searchDao = MainTestSuite.getSearchDao();
		searchDao.explain(deleteStatement).explain().get();
        searchDao.getClient().admin().indices().prepareRefresh(index).get();
	}
}
