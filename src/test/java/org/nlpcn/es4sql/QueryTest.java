package org.nlpcn.es4sql;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Assert;
import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.SqlElasticSearchRequestBuilder;

import java.io.IOException;
import java.sql.SQLFeatureNotSupportedException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.nlpcn.es4sql.TestsConstants.DATE_FORMAT;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_DOG;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_GAME_OF_THRONES;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_JOIN_TYPE;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_LOCATION;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_NESTED_TYPE;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_ODBC;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_ONLINE;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_PEOPLE;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_PHRASE;
import static org.nlpcn.es4sql.TestsConstants.TS_DATE_FORMAT;


public class QueryTest {

	@Test
	public void searchTypeTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
		SearchHits response = query(String.format("SELECT * FROM %s/phrase LIMIT 1000", TEST_INDEX_PHRASE));
		Assert.assertEquals(6, response.getTotalHits().value);
	}


    @Test
	public void multipleFromTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
		SearchHits response = query(String.format("SELECT * FROM %s/phrase, %s/account LIMIT 2000", TEST_INDEX_PHRASE, TEST_INDEX_ACCOUNT));
		Assert.assertEquals(1006, response.getTotalHits().value);
	}

	@Test
	public void indexWithWildcardTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
		SearchHits response = query("SELECT * FROM elasticsearch-* LIMIT 1000");
		assertThat(response.getTotalHits().value, greaterThan(0L));
	}


	@Test
	public void selectSpecificFields() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		String[] arr = new String[] {"age", "account_number"};
		Set expectedSource = new HashSet(Arrays.asList(arr));

		SearchHits response = query(String.format("SELECT age, account_number FROM %s/account", TEST_INDEX_ACCOUNT));
		SearchHit[] hits = response.getHits();
		for(SearchHit hit : hits) {
			Assert.assertEquals(expectedSource, hit.getSourceAsMap().keySet());
		}
	}

	@Test
	public void selectFieldWithSpace() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		String[] arr = new String[] {"test field"};
		Set expectedSource = new HashSet(Arrays.asList(arr));

		SearchHits response = query(String.format("SELECT ['test field'] FROM %s/phrase where ['test field'] is not null", TEST_INDEX_PHRASE));
		SearchHit[] hits = response.getHits();
		for(SearchHit hit : hits) {
			Assert.assertEquals(expectedSource, hit.getSourceAsMap().keySet());
		}
	}


	// TODO field aliases is not supported currently. it might be possible to change field names after the query already executed.
	/*
	@Test
	public void selectAliases() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		String[] arr = new String[] {"myage", "myaccount_number"};
		Set expectedSource = new HashSet(Arrays.asList(arr));

		SearchHits response = query(String.format("SELECT age AS myage, account_number AS myaccount_number FROM %s/account", TEST_INDEX));
		SearchHit[] hits = response.getHits();
		for(SearchHit hit : hits) {
			Assert.assertEquals(expectedSource, hit.getSource().keySet());
		}
	}
	*/


	@Test
	public void equallityTest() throws SqlParseException, SQLFeatureNotSupportedException {
		SearchHits response = query(String.format("select * from %s/account where city = 'Nogal' LIMIT 1000", TEST_INDEX_ACCOUNT));
		SearchHit[] hits = response.getHits();

		// assert the results is correct according to accounts.json data.
		Assert.assertEquals(1, response.getTotalHits().value);
		Assert.assertEquals("Nogal", hits[0].getSourceAsMap().get("city"));
	}


	// TODO search 'quick fox' still matching 'quick fox brown' this is wrong behavior.
	// in some cases, depends on the analasis, we might want choose better behavior for equallity.
	@Test
	public void equallityTest_phrase() throws SqlParseException, SQLFeatureNotSupportedException {
		SearchHits response = query(String.format("SELECT * FROM %s/phrase WHERE phrase = 'quick fox here' LIMIT 1000", TEST_INDEX_PHRASE));
		SearchHit[] hits = response.getHits();

		// assert the results is correct according to accounts.json data.
		Assert.assertEquals(1, response.getTotalHits().value);
		Assert.assertEquals("quick fox here", hits[0].getSourceAsMap().get("phrase"));
	}


	@Test
	public void greaterThanTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		int someAge = 25;
		SearchHits response = query(String.format("SELECT * FROM %s WHERE age > %s LIMIT 1000", TEST_INDEX_PEOPLE, someAge));
		SearchHit[] hits = response.getHits();
		for(SearchHit hit : hits) {
			int age = (int) hit.getSourceAsMap().get("age");
			assertThat(age, greaterThan(someAge));
		}
	}


	@Test
	public void greaterThanOrEqualTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		int someAge = 25;
		SearchHits response = query(String.format("SELECT * FROM %s WHERE age >= %s LIMIT 1000", TEST_INDEX_ACCOUNT, someAge));
		SearchHit[] hits = response.getHits();

		boolean isEqualFound = false;
		for(SearchHit hit : hits) {
			int age = (int) hit.getSourceAsMap().get("age");
			assertThat(age, greaterThanOrEqualTo(someAge));

			if(age == someAge)
				isEqualFound = true;
		}

		Assert.assertTrue(String.format("at least one of the documents need to contains age equal to %s", someAge), isEqualFound);
	}


	@Test
	public void lessThanTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		int someAge = 25;
		SearchHits response = query(String.format("SELECT * FROM %s WHERE age < %s LIMIT 1000", TEST_INDEX_PEOPLE, someAge));
		SearchHit[] hits = response.getHits();
		for(SearchHit hit : hits) {
			int age = (int) hit.getSourceAsMap().get("age");
			assertThat(age, lessThan(someAge));
		}
	}


	@Test
	public void lessThanOrEqualTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		int someAge = 25;
		SearchHits response = query(String.format("SELECT * FROM %s WHERE age <= %s LIMIT 1000", TEST_INDEX_ACCOUNT, someAge));
		SearchHit[] hits = response.getHits();

		boolean isEqualFound = false;
		for(SearchHit hit : hits) {
			int age = (int) hit.getSourceAsMap().get("age");
			assertThat(age, lessThanOrEqualTo(someAge));

			if(age == someAge)
				isEqualFound = true;
		}

		Assert.assertTrue(String.format("at least one of the documents need to contains age equal to %s", someAge), isEqualFound);
	}


	@Test
	public void orTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		SearchHits response = query(String.format("SELECT * FROM %s/account WHERE gender='F' OR gender='M' LIMIT 1000", TEST_INDEX_ACCOUNT));
		// Assert all documents from accounts.json is returned.
		Assert.assertEquals(1000, response.getTotalHits().value);
	}


	@Test
	public void andTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		SearchHits response = query(String.format("SELECT * FROM %s WHERE age=32 AND gender='M' LIMIT 1000", TEST_INDEX_PEOPLE));
		SearchHit[] hits = response.getHits();
		for(SearchHit hit : hits) {
			Assert.assertEquals(32, hit.getSourceAsMap().get("age"));
			Assert.assertEquals("M", hit.getSourceAsMap().get("gender"));
		}
	}


	@Test
	public void likeTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		SearchHits response = query(String.format("SELECT * FROM %s WHERE firstname LIKE 'amb%%' LIMIT 1000", TEST_INDEX_ACCOUNT));
		SearchHit[] hits = response.getHits();

		// assert the results is correct according to accounts.json data.
		Assert.assertEquals(1, response.getTotalHits().value);
		Assert.assertEquals("Amber", hits[0].getSourceAsMap().get("firstname"));
	}

	@Test
	public void notLikeTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		SearchHits response = query(String.format("SELECT * FROM %s/account WHERE firstname NOT LIKE 'amb%%'", TEST_INDEX_ACCOUNT));
		SearchHit[] hits = response.getHits();

		// assert we got hits
		Assert.assertNotEquals(0, response.getTotalHits().value);
		for (SearchHit hit : hits) {
			Assert.assertFalse(hit.getSourceAsMap().get("firstname").toString().toLowerCase().startsWith("amb"));
		}
	}

    @Test
    public void regexpQueryTest() throws SqlParseException, SQLFeatureNotSupportedException {
        String query = String.format("select * from %s/dog where dog_name = REGEXP_QUERY('sn.*', 'INTERSECTION|COMPLEMENT|EMPTY', 10000)", TEST_INDEX_DOG);
        SearchHit[] hits = query(query).getHits();
        Assert.assertEquals(1, hits.length);
        Map<String, Object> hitAsMap = hits[0].getSourceAsMap();
        Assert.assertEquals("snoopy", hitAsMap.get("dog_name"));
        Assert.assertEquals("Hattie", hitAsMap.get("holdersName"));
        Assert.assertEquals(4, hitAsMap.get("age"));
    }

	@Test
	public void doubleNotTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		SearchHits response1 = query(String.format("SELECT * FROM %s/account WHERE not gender like 'm' and not gender like 'f'", TEST_INDEX_ACCOUNT));
		Assert.assertEquals(0, response1.getTotalHits().value);

		SearchHits response2 = query(String.format("SELECT * FROM %s/account WHERE not gender like 'm' and gender not like 'f'", TEST_INDEX_ACCOUNT));
		Assert.assertEquals(0, response2.getTotalHits().value);

		SearchHits response3 = query(String.format("SELECT * FROM %s/account WHERE gender not like 'm' and gender not like 'f'", TEST_INDEX_ACCOUNT));
		Assert.assertEquals(0, response3.getTotalHits().value);

		SearchHits response4 = query(String.format("SELECT * FROM %s/account WHERE gender like 'm' and not gender like 'f'", TEST_INDEX_ACCOUNT));
		// assert there are results and they all have gender 'm'
		Assert.assertNotEquals(0, response4.getTotalHits().value);
		for (SearchHit hit : response4.getHits()) {
			Assert.assertEquals("m", hit.getSourceAsMap().get("gender").toString().toLowerCase());
		}

		SearchHits response5 = query(String.format("SELECT * FROM %s/account WHERE NOT (gender = 'm' OR gender = 'f')", TEST_INDEX_ACCOUNT));
		Assert.assertEquals(0, response5.getTotalHits().value);
	}

	@Test
	public void limitTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		SearchHits response = query(String.format("SELECT * FROM %s LIMIT 30", TEST_INDEX_ACCOUNT));
		SearchHit[] hits = response.getHits();

		// assert the results is correct according to accounts.json data.
		Assert.assertEquals(30, hits.length);
	}


	@Test
	public void betweenTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		int min = 27;
		int max = 30;
		SearchHits response = query(String.format("SELECT * FROM %s WHERE age BETWEEN %s AND %s LIMIT 1000", TEST_INDEX_PEOPLE, min, max));
		SearchHit[] hits = response.getHits();
		for(SearchHit hit : hits) {
			int age = (int) hit.getSourceAsMap().get("age");
			assertThat(age, allOf(greaterThanOrEqualTo(min), lessThanOrEqualTo(max)));
		}
	}


	/*
	TODO when using not between on some field, documents that not contains this
	 field will return as well, That may considered a Wrong behaivor.
	 */
	@Test
	public void notBetweenTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		int min = 20;
		int max = 37;
		SearchHits response = query(String.format("SELECT * FROM %s WHERE age NOT BETWEEN %s AND %s LIMIT 1000", TEST_INDEX_PEOPLE, min, max));
		SearchHit[] hits = response.getHits();
		for(SearchHit hit : hits) {
			Map<String, Object> source = hit.getSourceAsMap();

			// ignore document which not contains the age field.
			if(source.containsKey("age")) {
				int age = (int) hit.getSourceAsMap().get("age");
				assertThat(age, not(allOf(greaterThanOrEqualTo(min), lessThanOrEqualTo(max))));
			}
		}
	}


	@Test
	public void inTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
		SearchHits response = query(String.format("SELECT age FROM %s/phrase WHERE age IN (20, 22) LIMIT 1000", TEST_INDEX_PHRASE));
		SearchHit[] hits = response.getHits();
		for(SearchHit hit : hits) {
			int age = (int) hit.getSourceAsMap().get("age");
			assertThat(age, isOneOf(20, 22));
		}
	}


	@Test
	public void inTestWithStrings() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
		SearchHits response = query(String.format("SELECT phrase FROM %s/phrase WHERE phrase IN ('quick fox here', 'fox brown') LIMIT 1000", TEST_INDEX_PHRASE));
		SearchHit[] hits = response.getHits();
		Assert.assertEquals(2, response.getTotalHits().value);
		for(SearchHit hit : hits) {
			String phrase = (String) hit.getSourceAsMap().get("phrase");
			assertThat(phrase, isOneOf("quick fox here", "fox brown"));
		}
	}

    @Test
    public void inTermsTestWithIdentifiersTreatLikeStrings() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SearchHits response = query(String.format("SELECT name FROM %s/gotCharacters WHERE name.firstname = IN_TERMS(daenerys,eddard) LIMIT 1000", TEST_INDEX_GAME_OF_THRONES));
        SearchHit[] hits = response.getHits();
        Assert.assertEquals(2, response.getTotalHits().value);
        for(SearchHit hit : hits) {
            String firstname =  ((Map<String,Object>) hit.getSourceAsMap().get("name")).get("firstname").toString();
            assertThat(firstname, isOneOf("Daenerys", "Eddard"));
        }
    }
    @Test
    public void inTermsTestWithStrings() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SearchHits response = query(String.format("SELECT name FROM %s/gotCharacters WHERE name.firstname = IN_TERMS('daenerys','eddard') LIMIT 1000", TEST_INDEX_GAME_OF_THRONES));
        SearchHit[] hits = response.getHits();
        Assert.assertEquals(2, response.getTotalHits().value);
        for(SearchHit hit : hits) {
            String firstname =  ((Map<String,Object>) hit.getSourceAsMap().get("name")).get("firstname").toString();
            assertThat(firstname, isOneOf("Daenerys", "Eddard"));
        }
    }

    @Test
    public void inTermsTestWithNumbers() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SearchHits response = query(String.format("SELECT name FROM %s/gotCharacters WHERE name.ofHisName = IN_TERMS(4,2) LIMIT 1000", TEST_INDEX_GAME_OF_THRONES));
        SearchHit[] hits = response.getHits();
        Assert.assertEquals(1, response.getTotalHits().value);
        SearchHit hit = hits[0];
        String firstname =  ((Map<String,Object>) hit.getSourceAsMap().get("name")).get("firstname").toString();
        Assert.assertEquals("Brandon",firstname);
    }


    @Test
    public void termQueryWithNumber() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SearchHits response = query(String.format("SELECT name FROM %s/gotCharacters WHERE name.ofHisName = term(4) LIMIT 1000", TEST_INDEX_GAME_OF_THRONES));
        SearchHit[] hits = response.getHits();
        Assert.assertEquals(1, response.getTotalHits().value);
        SearchHit hit = hits[0];
        String firstname =  ((Map<String,Object>) hit.getSourceAsMap().get("name")).get("firstname").toString();
        Assert.assertEquals("Brandon",firstname);
    }

    @Test
    public void termQueryWithStringIdentifier() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SearchHits response = query(String.format("SELECT name FROM %s/gotCharacters WHERE name.firstname = term(brandon) LIMIT 1000", TEST_INDEX_GAME_OF_THRONES));
        SearchHit[] hits = response.getHits();
        Assert.assertEquals(1, response.getTotalHits().value);
        SearchHit hit = hits[0];
        String firstname =  ((Map<String,Object>) hit.getSourceAsMap().get("name")).get("firstname").toString();
        Assert.assertEquals("Brandon",firstname);
    }

    @Test
    public void termQueryWithStringLiteral() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SearchHits response = query(String.format("SELECT name FROM %s/gotCharacters WHERE name.firstname = term('brandon') LIMIT 1000", TEST_INDEX_GAME_OF_THRONES));
        SearchHit[] hits = response.getHits();
        Assert.assertEquals(1, response.getTotalHits().value);
        SearchHit hit = hits[0];
        String firstname =  ((Map<String,Object>) hit.getSourceAsMap().get("name")).get("firstname").toString();
        Assert.assertEquals("Brandon",firstname);
    }


    /* TODO when using not in on some field, documents that not contains this
	field will return as well, That may considered a Wrong behaivor.
	*/
	@Test
	public void notInTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		SearchHits response = query(String.format("SELECT age FROM %s WHERE age NOT IN (20, 22) LIMIT 1000", TEST_INDEX_PEOPLE));
		SearchHit[] hits = response.getHits();
		for(SearchHit hit : hits) {
			Map<String, Object> source = hit.getSourceAsMap();

			// ignore document which not contains the age field.
			if(source.containsKey("age")) {
				int age = (int) source.get("age");
				assertThat(age, not(isOneOf(20, 22)));
			}
		}
	}


	@Test
	public void dateSearch() throws IOException, SqlParseException, SQLFeatureNotSupportedException, ParseException {
		DateTimeFormatter formatter = DateTimeFormat.forPattern(DATE_FORMAT);
		DateTime dateToCompare = new DateTime(2014, 8, 18, 0, 0, 0);

		SearchHits response = query(String.format("SELECT insert_time FROM %s/online WHERE insert_time < '2014-08-18'", TEST_INDEX_ONLINE));
		SearchHit[] hits = response.getHits();
		for(SearchHit hit : hits) {
			Map<String, Object> source = hit.getSourceAsMap();
			DateTime insertTime = formatter.parseDateTime((String) source.get("insert_time"));

			String errorMessage = String.format("insert_time must be smaller then 2014-08-18. found: %s", insertTime);
			Assert.assertTrue(errorMessage, insertTime.isBefore(dateToCompare));
		}
	}

    @Test
    public void dateSearchBraces() throws IOException, SqlParseException, SQLFeatureNotSupportedException, ParseException {
        DateTimeFormatter formatter = DateTimeFormat.forPattern(TS_DATE_FORMAT);
        DateTime dateToCompare = new DateTime(2015, 3, 15, 0, 0, 0);

        SearchHits response = query(String.format("SELECT odbc_time FROM %s/odbc WHERE odbc_time < {ts '2015-03-15 00:00:00.000'}", TEST_INDEX_ODBC));
        SearchHit[] hits = response.getHits();
        for(SearchHit hit : hits) {
            Map<String, Object> source = hit.getSourceAsMap();
			String insertTimeStr = (String) source.get("odbc_time");
			insertTimeStr = insertTimeStr.replace("{ts '", "").replace("'}", "");

            DateTime insertTime = formatter.parseDateTime(insertTimeStr);

            String errorMessage = String.format("insert_time must be smaller then 2015-03-15. found: %s", insertTime);
            Assert.assertTrue(errorMessage, insertTime.isBefore(dateToCompare));
        }
    }


	@Test
	public void dateBetweenSearch() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		DateTimeFormatter formatter = DateTimeFormat.forPattern(DATE_FORMAT);

		DateTime dateLimit1 = new DateTime(2014, 8, 18, 0, 0, 0);
		DateTime dateLimit2 = new DateTime(2014, 8, 21, 0, 0, 0);

		SearchHits response = query(String.format("SELECT insert_time FROM %s/online WHERE insert_time BETWEEN '2014-08-18' AND '2014-08-21' LIMIT 3", TEST_INDEX_ONLINE));
		SearchHit[] hits = response.getHits();
		for(SearchHit hit : hits) {
			Map<String, Object> source = hit.getSourceAsMap();
			DateTime insertTime = formatter.parseDateTime((String) source.get("insert_time"));

			boolean isBetween =
					(insertTime.isAfter(dateLimit1) || insertTime.isEqual(dateLimit1)) &&
					(insertTime.isBefore(dateLimit2) || insertTime.isEqual(dateLimit2));

			Assert.assertTrue("insert_time must be between 2014-08-18 and 2014-08-21", isBetween);
		}
	}


	@Test
	public void missFilterSearch() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
		SearchHits response = query(String.format("SELECT * FROM %s/phrase WHERE insert_time2 IS missing", TEST_INDEX_PHRASE));
		SearchHit[] hits = response.getHits();

		// should be 2 according to the data.
		Assert.assertEquals(response.getTotalHits().value, 4);
		for(SearchHit hit : hits) {
			assertThat(hit.getSourceAsMap(), not(hasKey("insert_time2")));
		}
	}

	@Test
	public void notMissFilterSearch() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
		SearchHits response = query(String.format("SELECT * FROM %s/phrase WHERE insert_time2 IS NOT missing", TEST_INDEX_PHRASE));
		SearchHit[] hits = response.getHits();

		// should be 2 according to the data.
		Assert.assertEquals(response.getTotalHits().value, 2);
		for(SearchHit hit : hits) {
			assertThat(hit.getSourceAsMap(), hasKey("insert_time2"));
		}
	}



	@Test
	public void complexConditionQuery() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
		String errorMessage = "Result does not exist to the condition (gender='m' AND (age> 25 OR account_number>5)) OR (gender='f' AND (age>30 OR account_number < 8)";

		SearchHits response = query(String.format("SELECT * FROM %s/account WHERE (gender='m' AND (age> 25 OR account_number>5)) OR (gender='f' AND (age>30 OR account_number < 8))", TEST_INDEX_ACCOUNT));
		SearchHit[] hits = response.getHits();

		for(SearchHit hit : hits) {
			Map<String, Object> source = hit.getSourceAsMap();
			String gender = ((String)source.get("gender")).toLowerCase();
			int age = (int)source.get("age");
			int account_number = (int) source.get("account_number");

			Assert.assertTrue(errorMessage, (gender.equals("m") && (age> 25 || account_number>5)) || (gender.equals("f") && (age>30 || account_number < 8)));
		}
	}

	@Test
	public void complexNotConditionQuery() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		String errorMessage = "Result does not exist to the condition NOT (gender='m' AND NOT (age > 25 OR account_number > 5)) OR (NOT gender='f' AND NOT (age > 30 OR account_number < 8))";

		SearchHits response = query(String.format("SELECT * FROM %s/account WHERE NOT (gender='m' AND NOT (age > 25 OR account_number > 5)) OR (NOT gender='f' AND NOT (age > 30 OR account_number < 8))", TEST_INDEX_ACCOUNT));
		SearchHit[] hits = response.getHits();

		Assert.assertNotEquals(hits.length, 0);

		for (SearchHit hit : hits) {
			Map<String, Object> source = hit.getSourceAsMap();
			String gender = ((String) source.get("gender")).toLowerCase();
			int age = (int) source.get("age");
			int account_number = (int) source.get("account_number");

			Assert.assertTrue(errorMessage, !(gender.equals("m") && !(age > 25 || account_number > 5)) || (!gender.equals("f") && !(age > 30 || account_number < 8)));
		}
	}

	@Test
	public void orderByAscTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		SearchHits response = query(String.format("SELECT age FROM %s/account ORDER BY age ASC LIMIT 1000", TEST_INDEX_ACCOUNT));
		SearchHit[] hits = response.getHits();

		ArrayList<Integer> ages = new ArrayList<Integer>();
		for(SearchHit hit : hits) {
			ages.add((int)hit.getSourceAsMap().get("age"));
		}

		ArrayList<Integer> sortedAges = (ArrayList<Integer>)ages.clone();
		Collections.sort(sortedAges);
		Assert.assertTrue("The list is not ordered ascending", sortedAges.equals(ages));
	}


	@Test
	public void orderByDescTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		SearchHits response = query(String.format("SELECT age FROM %s/account ORDER BY age DESC LIMIT 1000", TEST_INDEX_ACCOUNT));
		SearchHit[] hits = response.getHits();

		ArrayList<Integer> ages = new ArrayList<Integer>();
		for(SearchHit hit : hits) {
			ages.add((int)hit.getSourceAsMap().get("age"));
		}

		ArrayList<Integer> sortedAges = (ArrayList<Integer>)ages.clone();
		Collections.sort(sortedAges, Collections.reverseOrder());
		Assert.assertTrue("The list is not ordered descending", sortedAges.equals(ages));
	}


	@Test
	public void orderByAscFieldWithSpaceTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		SearchHits response = query(String.format("SELECT * FROM %s/phrase where `test field` is not null ORDER BY `test field` ASC LIMIT 1000", TEST_INDEX_PHRASE));
		SearchHit[] hits = response.getHits();

		ArrayList<Integer> testFields = new ArrayList<Integer>();
		for(SearchHit hit : hits) {
			testFields.add((int)hit.getSourceAsMap().get("test field"));
		}

		ArrayList<Integer> sortedTestFields = (ArrayList<Integer>)testFields.clone();
		Collections.sort(sortedTestFields);
		Assert.assertTrue("The list is not ordered ascending", sortedTestFields.equals(testFields));
	}

    @Test
    public void testMultipartWhere() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SearchHits response = query(String.format("SELECT * FROM %s/account WHERE (firstname LIKE 'opal' OR firstname like 'rodriquez') AND (state like 'oh' OR state like 'hi')", TEST_INDEX_ACCOUNT));
        Assert.assertEquals(2, response.getTotalHits().value);
    }

    @Test
    public void testMultipartWhere2() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SearchHits response = query(String.format("SELECT * FROM %s/account where ((account_number > 200 and account_number < 300) or gender like 'm') and (state like 'hi' or address like 'avenue')", TEST_INDEX_ACCOUNT));
        Assert.assertEquals(127, response.getTotalHits().value);
    }

    @Test
    public void testMultipartWhere3() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SearchHits response = query(String.format("SELECT * FROM %s/account where ((account_number > 25 and account_number < 75) and age >35 ) and (state like 'md' or (address like 'avenue' or address like 'street'))", TEST_INDEX_ACCOUNT));
        Assert.assertEquals(7, response.getTotalHits().value);
    }

    @Test
    public void filterPolygonTest() throws SQLFeatureNotSupportedException, SqlParseException, InterruptedException {
        SearchHits results = query(String.format("SELECT * FROM %s/location WHERE GEO_INTERSECTS(place,'POLYGON ((102 2, 103 2, 103 3, 102 3, 102 2))')", TEST_INDEX_LOCATION));
        org.junit.Assert.assertEquals(1,results.getTotalHits().value);
        SearchHit result = results.getAt(0);
        Assert.assertEquals("bigSquare",result.getSourceAsMap().get("description"));
    }

    @Test
    public void boundingBox() throws SQLFeatureNotSupportedException, SqlParseException, InterruptedException {
        SearchHits results = query(String.format("SELECT * FROM %s/location WHERE GEO_BOUNDING_BOX(center,100.0,1.0,101,0.0)", TEST_INDEX_LOCATION));
        org.junit.Assert.assertEquals(1,results.getTotalHits().value);
        SearchHit result = results.getAt(0);
        Assert.assertEquals("square",result.getSourceAsMap().get("description"));
    }
    @Test
    public void geoDistance() throws SQLFeatureNotSupportedException, SqlParseException, InterruptedException {
        SearchHits results = query(String.format("SELECT * FROM %s/location WHERE GEO_DISTANCE(center,'1km',100.5,0.500001)", TEST_INDEX_LOCATION));
        org.junit.Assert.assertEquals(1,results.getTotalHits().value);
        SearchHit result = results.getAt(0);
        Assert.assertEquals("square",result.getSourceAsMap().get("description"));
    }

    //ES5.0: geo_distance_range] queries are no longer supported for geo_point field types. Use geo_distance sort or aggregations
//    @Test
//    public void geoDistanceRange() throws SQLFeatureNotSupportedException, SqlParseException, InterruptedException {
//        SearchHits results = query(String.format("SELECT * FROM %s/location WHERE GEO_DISTANCE_RANGE(center,'1m','1km',100.5,0.50001)", TEST_INDEX));
//        org.junit.Assert.assertEquals(1,results.getTotalHits());
//        SearchHit result = results.getAt(0);
//        Assert.assertEquals("square",result.getSource().get("description"));
//    }

    //ES5.0: geo_point field no longer supports geohash_cell queries
//    @Test
//    public void geoCell() throws SQLFeatureNotSupportedException, SqlParseException, InterruptedException {
//        SearchHits results = query(String.format("SELECT * FROM %s/location WHERE GEO_CELL(center,100.5,0.50001,7)", TEST_INDEX));
//        org.junit.Assert.assertEquals(1,results.getTotalHits());
//        SearchHit result = results.getAt(0);
//        Assert.assertEquals("square",result.getSource().get("description"));
//    }

    @Test
    public void geoPolygon() throws SQLFeatureNotSupportedException, SqlParseException, InterruptedException {
        SearchHits results = query(String.format("SELECT * FROM %s/location WHERE GEO_POLYGON(center,100,0,100.5,2,101.0,0)", TEST_INDEX_LOCATION));
        org.junit.Assert.assertEquals(1,results.getTotalHits().value);
        SearchHit result = results.getAt(0);
        Assert.assertEquals("square",result.getSourceAsMap().get("description"));
    }

    @Test
    public void escapedCharactersCheck() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SearchHits response = query(String.format("SELECT * FROM %s/gotCharacters where nickname = 'Daenerys \"Stormborn\"' LIMIT 1000", TEST_INDEX_GAME_OF_THRONES));
        Assert.assertEquals(1, response.getTotalHits().value);
    }

    @Test
    public void complexObjectSearch() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SearchHits response = query(String.format("SELECT * FROM %s/gotCharacters where name.firstname = 'Jaime' LIMIT 1000", TEST_INDEX_GAME_OF_THRONES));
        Assert.assertEquals(1, response.getTotalHits().value);
    }

    @Test
    public void complexObjectReutrnField() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SearchHits response = query(String.format("SELECT parents.father FROM %s/gotCharacters where name.firstname = 'Brandon' LIMIT 1000", TEST_INDEX_GAME_OF_THRONES));
        Assert.assertEquals(1, response.getTotalHits().value);
        Map<String, Object> sourceAsMap = response.getHits()[0].getSourceAsMap();
        Assert.assertEquals("Eddard",((HashMap<String,Object>)sourceAsMap.get("parents")).get("father"));
    }

    @Test
    public void queryWithATfieldOnWhere() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SearchHits response = query(String.format("SELECT * FROM %s/gotCharacters where @wolf = 'Summer' LIMIT 1000", TEST_INDEX_GAME_OF_THRONES));
        Assert.assertEquals(1, response.getTotalHits().value);
        Map<String, Object> sourceAsMap = response.getHits()[0].getSourceAsMap();
        Assert.assertEquals("Summer",sourceAsMap.get("@wolf"));
        Assert.assertEquals("Brandon",((HashMap<String,Object>)sourceAsMap.get("name")).get("firstname"));
    }

    @Test
    public void notLikeTests() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        //cant use string.format cause of %d
        SearchHits response = query("SELECT name FROM " +TEST_INDEX_GAME_OF_THRONES + "/gotCharacters where name.firstname not like '%d' and name is not null LIMIT 1000");
        Assert.assertEquals(3, response.getTotalHits().value);
        for(SearchHit hit : response.getHits()) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = ((HashMap<String, Object>) sourceAsMap.get("name")).get("firstname").toString();
            Assert.assertFalse(name+" was in not like %d",name.startsWith("d"));
        }
    }

    @Test
    public void isNullTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SearchHits response = query("SELECT name FROM " +TEST_INDEX_GAME_OF_THRONES + "/gotCharacters where nickname IS NULL LIMIT 1000");
        Assert.assertEquals(6, response.getTotalHits().value);
    }

    @Test
    public void isNotNullTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SearchHits response = query("SELECT name FROM " +TEST_INDEX_GAME_OF_THRONES + "/gotCharacters where nickname IS NOT NULL LIMIT 1000");
        Assert.assertEquals(1, response.getTotalHits().value);
    }


    @Test
    public void useScrollNoParams() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SearchResponse response = getSearchResponse(String.format("SELECT /*! USE_SCROLL*/ age,gender,firstname,balance FROM  %s/account LIMIT 2000", TEST_INDEX_ACCOUNT));
        Assert.assertNotNull(response.getScrollId());
        SearchHits hits = response.getHits();
        //default is 50 , es5.0 functionality now returns docs on first scroll
        Assert.assertEquals(50,hits.getHits().length);
        Assert.assertEquals(1000,hits.getTotalHits().value);
    }

    @Test
    public void useScrollWithParams() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SearchResponse response = getSearchResponse(String.format("SELECT /*! USE_SCROLL(10,5000)*/ age,gender,firstname,balance FROM  %s/account ", TEST_INDEX_ACCOUNT));
        Assert.assertNotNull(response.getScrollId());
        SearchHits hits = response.getHits();
        Assert.assertEquals(10,hits.getHits().length);
        Assert.assertEquals(1000,hits.getTotalHits().value);
    }


    @Test
    public void useScrollWithOrderByAndParams() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SearchResponse response = getSearchResponse(String.format("SELECT /*! USE_SCROLL(5,50000)*/ age,gender,firstname,balance FROM  %s/account order by age", TEST_INDEX_ACCOUNT));
        Assert.assertNotNull(response.getScrollId());
        SearchHits hits = response.getHits();
        Assert.assertEquals(5,hits.getHits().length);
        Assert.assertEquals(1000,hits.getTotalHits().value);
        for(SearchHit hit : hits){
            Assert.assertEquals(20,hit.getSourceAsMap().get("age"));
        }
    }

    @Test
    public void innerQueryTest() throws SqlParseException, SQLFeatureNotSupportedException {
        String query = String.format("select * from %s/dog where holdersName IN (select firstname from %s/account where firstname = 'Hattie')",TEST_INDEX_DOG,TEST_INDEX_ACCOUNT);
        SearchHit[] hits = query(query).getHits();
        Assert.assertEquals(1,hits.length);
        Map<String, Object> hitAsMap = hits[0].getSourceAsMap();
        Assert.assertEquals("snoopy",hitAsMap.get("dog_name"));
        Assert.assertEquals("Hattie",hitAsMap.get("holdersName"));
        Assert.assertEquals(4,hitAsMap.get("age"));

    }

    @Test
    public void twoSubQueriesTest() throws SqlParseException, SQLFeatureNotSupportedException {
        String query = String.format("select * from %s/dog where holdersName IN (select firstname from %s/account where firstname = 'Hattie') and age IN (select name.ofHisName from %s/gotCharacters where name.firstname <> 'Daenerys' and name.ofHisName IS NOT NULL) ",TEST_INDEX_DOG,TEST_INDEX_ACCOUNT,TEST_INDEX_GAME_OF_THRONES);
        SearchHit[] hits = query(query).getHits();
        Assert.assertEquals(1,hits.length);
        Map<String, Object> hitAsMap = hits[0].getSourceAsMap();
        Assert.assertEquals("snoopy",hitAsMap.get("dog_name"));
        Assert.assertEquals("Hattie",hitAsMap.get("holdersName"));
        Assert.assertEquals(4,hitAsMap.get("age"));

    }

    @Test
    public void inTermsSubQueryTest() throws SqlParseException, SQLFeatureNotSupportedException {
        String query = String.format("select * from %s/dog where age = IN_TERMS (select name.ofHisName from %s/gotCharacters where name.firstname <> 'Daenerys' and name.ofHisName is not null)",TEST_INDEX_DOG,TEST_INDEX_GAME_OF_THRONES);
        SearchHit[] hits = query(query).getHits();
        Assert.assertEquals(1,hits.length);
        Map<String, Object> hitAsMap = hits[0].getSourceAsMap();
        Assert.assertEquals("snoopy",hitAsMap.get("dog_name"));
        Assert.assertEquals("Hattie",hitAsMap.get("holdersName"));
        Assert.assertEquals(4, hitAsMap.get("age"));

    }

    @Test
    public void idsQueryOneId() throws SqlParseException, SQLFeatureNotSupportedException {
        String query = String.format("select * from %s/dog where _id = IDS_QUERY(dog,1)",TEST_INDEX_DOG);
        SearchHit[] hits = query(query).getHits();
        Assert.assertEquals(1,hits.length);
        Map<String, Object> hitAsMap = hits[0].getSourceAsMap();
        Assert.assertEquals("rex",hitAsMap.get("dog_name"));
        Assert.assertEquals("Daenerys",hitAsMap.get("holdersName"));
        Assert.assertEquals(2, hitAsMap.get("age"));

    }

    @Test
    public void idsQueryMultipleId() throws SqlParseException, SQLFeatureNotSupportedException {
        String query = String.format("select * from %s/dog where _id = IDS_QUERY(dog,1,2,3)",TEST_INDEX_DOG);
        SearchHit[] hits = query(query).getHits();
        Assert.assertEquals(1,hits.length);
        Map<String, Object> hitAsMap = hits[0].getSourceAsMap();
        Assert.assertEquals("rex",hitAsMap.get("dog_name"));
        Assert.assertEquals("Daenerys",hitAsMap.get("holdersName"));
        Assert.assertEquals(2, hitAsMap.get("age"));

    }

    @Test
    public void idsQuerySubQueryIds() throws SqlParseException, SQLFeatureNotSupportedException {
        String query = String.format("select * from %s/dog where _id = IDS_QUERY(dog,(select name.ofHisName from %s/gotCharacters where name.firstname <> 'Daenerys' and name.ofHisName is not null))",TEST_INDEX_DOG,TEST_INDEX_GAME_OF_THRONES);
        SearchHit[] hits = query(query).getHits();
        Assert.assertEquals(1,hits.length);
        Map<String, Object> hitAsMap = hits[0].getSourceAsMap();
        Assert.assertEquals("rex",hitAsMap.get("dog_name"));
        Assert.assertEquals("Daenerys",hitAsMap.get("holdersName"));
        Assert.assertEquals(2, hitAsMap.get("age"));

    }

    @Test
    public void nestedEqualsTestFieldNormalField() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SearchHits response = query(String.format("SELECT * FROM %s/nestedType where nested(message.info)='b'", TEST_INDEX_NESTED_TYPE));
        Assert.assertEquals(1, response.getTotalHits().value);
    }

    @Test
    public void nestedEqualsTestFieldInsideArrays() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SearchHits response = query(String.format("SELECT * FROM %s/nestedType where nested(message.info) = 'a'", TEST_INDEX_NESTED_TYPE));
        Assert.assertEquals(2, response.getTotalHits().value);
    }

//    @Test
//    public void nestedOnInQuery() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
//        SearchHits response = query(String.format("SELECT * FROM %s/nestedType where nested(message.info) in ('a','b')", TEST_INDEX));
//        Assert.assertEquals(3, response.getTotalHits());
//    }

    @Test
    public void complexNestedQueryBothOnSameObject() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SearchHits response = query(String.format("SELECT * FROM %s/nestedType where nested('message',message.info = 'a' and message.author ='i' ) ", TEST_INDEX_NESTED_TYPE));
        Assert.assertEquals(1, response.getTotalHits().value);
    }

    @Test
    public void complexNestedQueryNotBothOnSameObject() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SearchHits response = query(String.format("SELECT * FROM %s/nestedType where nested('message',message.info = 'a' and message.author ='h' ) ", TEST_INDEX_NESTED_TYPE));
        Assert.assertEquals(0, response.getTotalHits().value);
    }

    @Test
    public void nestedOnInTermsQuery() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SearchHits response = query(String.format("SELECT * FROM %s/nestedType where nested(message.info) = IN_TERMS(a,b)", TEST_INDEX_NESTED_TYPE));
        Assert.assertEquals(3, response.getTotalHits().value);
    }

    @Test
    public void childrenEqualsTestFieldNormalField() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SearchHits response = query(String.format("SELECT * FROM %s/joinType where children(childrenType, info)='b'", TEST_INDEX_JOIN_TYPE));
        Assert.assertEquals(1, response.getTotalHits().value);
    }

    @Test
    public void childrenOnInQuery() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
    	SearchHits response = query(String.format("SELECT * FROM %s/joinType where children(childrenType, info) in ('a','b')", TEST_INDEX_JOIN_TYPE));
    	Assert.assertEquals(2, response.getTotalHits().value);
    }

    @Test
    public void complexChildrenQueryBothOnSameObject() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SearchHits response = query(String.format("SELECT * FROM %s/joinType where children(childrenType, info = 'a' and author ='e' ) ", TEST_INDEX_JOIN_TYPE));
        Assert.assertEquals(1, response.getTotalHits().value);
    }

    @Test
    public void complexChildrenQueryNotBothOnSameObject() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SearchHits response = query(String.format("SELECT * FROM %s/joinType where children(childrenType, info = 'a' and author ='j' ) ", TEST_INDEX_JOIN_TYPE));
        Assert.assertEquals(0, response.getTotalHits().value);
    }

    @Test
    public void childrenOnInTermsQuery() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SearchHits response = query(String.format("SELECT * FROM %s/joinType where children(childrenType, info) = IN_TERMS(a,b)", TEST_INDEX_JOIN_TYPE));
        Assert.assertEquals(2, response.getTotalHits().value);
    }

    @Test
    public void multipleIndicesOneNotExistWithHint() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SearchHits response = query(String.format("SELECT /*! IGNORE_UNAVAILABLE */ * FROM %s,%s ", TEST_INDEX_ACCOUNT,"badindex"));
        Assert.assertTrue(response.getTotalHits().value > 0);
    }

    @Test(expected=IndexNotFoundException.class)
    public void multipleIndicesOneNotExistWithoutHint() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SearchHits response = query(String.format("SELECT  * FROM %s,%s ", TEST_INDEX_ACCOUNT,"badindex"));
        Assert.assertTrue(response.getTotalHits().value > 0);
    }

    @Test
    public void routingRequestOneRounting() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SqlElasticSearchRequestBuilder request = getRequestBuilder(String.format("SELECT /*! ROUTINGS(hey) */ * FROM %s/account ", TEST_INDEX_ACCOUNT));
        SearchRequestBuilder searchRequestBuilder = (SearchRequestBuilder) request.getBuilder();
        Assert.assertEquals("hey",searchRequestBuilder.request().routing());
    }

    @Test
    public void routingRequestMultipleRountings() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SqlElasticSearchRequestBuilder request = getRequestBuilder(String.format("SELECT /*! ROUTINGS(hey,bye) */ * FROM %s/account ", TEST_INDEX_ACCOUNT));
        SearchRequestBuilder searchRequestBuilder = (SearchRequestBuilder) request.getBuilder();
        Assert.assertEquals("hey,bye",searchRequestBuilder.request().routing());
    }

    //todo: find a way to check if scripts are enabled , uncomment before deploy.
//    @Test
//    public void scriptFilterNoParams() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
//        SearchHits response = query(String.format("SELECT insert_time FROM %s/online where script('doc[\\'insert_time\''].date.hourOfDay==16') " +
//                "and insert_time <'2014-08-21T00:00:00.000Z'", TEST_INDEX));
//        Assert.assertEquals(237,response.getTotalHits() );
//
//    }
//
//    @Test
//    public void scriptFilterWithParams() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
//        SearchHits response = query(String.format("SELECT insert_time FROM %s/online where script('doc[\\'insert_time\''].date.hourOfDay==x','x'=16) " +
//                "and insert_time <'2014-08-21T00:00:00.000Z'", TEST_INDEX));
//        Assert.assertEquals(237,response.getTotalHits() );
//
//    }


    @Test
    public void highlightPreTagsAndPostTags() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        String query = String.format("select /*! HIGHLIGHT(phrase,pre_tags : ['<b>'], post_tags : ['</b>']  ) */ " +
                "* from %s/phrase " +
                "where phrase like 'fox' " +
                "order by _score", TEST_INDEX_PHRASE);
        SearchHits hits = query(query);
        for (SearchHit hit : hits){
            HighlightField phrase = hit.getHighlightFields().get("phrase");
            String highlightPhrase = phrase.getFragments()[0].string();
            Assert.assertTrue(highlightPhrase.contains("<b>fox</b>"));
        }

    }

    @Test
    public void fieldCollapsingTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
        String query = String.format("select /*! COLLAPSE({\"field\":\"age\",\"inner_hits\":{\"name\":\"account\",\"size\":1,\"sort\":[{\"age\":\"asc\"}]},\"max_concurrent_group_searches\": 4}) */ * from %s/account", TEST_INDEX_ACCOUNT);
        SearchHits hits = query(query);
        Assert.assertEquals(21, hits.getHits().length);
    }

	@Test
	public void runtimeMappingsTest() throws SqlParseException, SQLFeatureNotSupportedException {
		String query = String.format("select /*! RUNTIME_MAPPINGS({\"email_suffix\":{\"type\":\"keyword\",\"script\":{\"source\":\"def email=params['_source']['email'];emit(email.substring(email.indexOf('@')))\"}}})*/* from %s/account where email_suffix='@xurban.com'", TEST_INDEX_ACCOUNT);
		SearchHits hits = query(query);
		Assert.assertEquals(2, hits.getHits().length);
	}

    private SearchHits query(String query) throws SqlParseException, SQLFeatureNotSupportedException {
        SearchDao searchDao = MainTestSuite.getSearchDao();
        SqlElasticSearchRequestBuilder select = (SqlElasticSearchRequestBuilder) searchDao.explain(query).explain();
        return ((SearchResponse)select.get()).getHits();
    }


    private SqlElasticSearchRequestBuilder getRequestBuilder(String query) throws SqlParseException, SQLFeatureNotSupportedException {
        SearchDao searchDao = MainTestSuite.getSearchDao();
        return  (SqlElasticSearchRequestBuilder) searchDao.explain(query).explain();
    }

    private SearchResponse getSearchResponse(String query) throws SqlParseException, SQLFeatureNotSupportedException {
        SearchDao searchDao = MainTestSuite.getSearchDao();
        SqlElasticSearchRequestBuilder select = (SqlElasticSearchRequestBuilder) searchDao.explain(query).explain();
        return ((SearchResponse)select.get());
    }

}
