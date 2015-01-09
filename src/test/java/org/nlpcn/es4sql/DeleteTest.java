package org.nlpcn.es4sql;


import org.durid.sql.ast.statement.SQLDeleteStatement;
import org.durid.sql.parser.SQLParserUtils;
import org.durid.sql.parser.SQLStatementParser;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX;

public class DeleteTest {
	/*
	@Before
	public void loadTempData() throws Exception {
		MainTestSuite.loadBulk("src/test/resources/accounts.json");
	}

	@After
	public void deleteTempData() throws Exception {
		MainTestSuite.deleteQuery(TEST_INDEX, "account_temp");
	}
*/
	/*
	@Test
	public void deleteAllTest() throws IOException, SqlParseException {
		SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(String.format("DELETE FROM %s", TEST_INDEX), "mysql");
		SQLDeleteStatement delete = parser.parseDeleteStatement();
	}
	*/
}
