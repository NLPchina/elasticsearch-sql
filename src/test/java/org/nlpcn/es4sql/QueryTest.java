package org.nlpcn.es4sql;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;
import org.elasticsearch.common.joda.time.format.DateTimeFormatter;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.Assert;
import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.SqlElasticSearchRequestBuilder;

import javax.naming.directory.SearchControls;
import java.io.IOException;
import java.sql.SQLFeatureNotSupportedException;
import java.text.ParseException;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.nlpcn.es4sql.TestsConstants.DATE_FORMAT;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX;
import static org.nlpcn.es4sql.TestsConstants.TS_DATE_FORMAT;


public class QueryTest {

	@Test
	public void searchTypeTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
		SearchHits response = query(String.format("SELECT * FROM %s/phrase LIMIT 1000", TEST_INDEX));
		Assert.assertEquals(4, response.getTotalHits());
	}


    @Test
	public void multipleFromTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
		SearchHits response = query(String.format("SELECT * FROM %s/phrase, %s/account LIMIT 2000", TEST_INDEX, TEST_INDEX));
		Assert.assertEquals(1004, response.getTotalHits());
	}

	@Test
	public void indexWithWildcardTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
		SearchHits response = query("SELECT * FROM elasticsearch-* LIMIT 1000");
		assertThat(response.getTotalHits(), greaterThan(0L));
	}


	@Test
	public void selectSpecificFields() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		String[] arr = new String[] {"age", "account_number"};
		Set expectedSource = new HashSet(Arrays.asList(arr));

		SearchHits response = query(String.format("SELECT age, account_number FROM %s/account", TEST_INDEX));
		SearchHit[] hits = response.getHits();
		for(SearchHit hit : hits) {
			Assert.assertEquals(expectedSource, hit.getSource().keySet());
		}
	}

	@Test
	public void selectFieldWithSpace() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		String[] arr = new String[] {"test field"};
		Set expectedSource = new HashSet(Arrays.asList(arr));

		SearchHits response = query(String.format("SELECT `test field` FROM %s/phrase_2", TEST_INDEX));
		SearchHit[] hits = response.getHits();
		for(SearchHit hit : hits) {
			Assert.assertEquals(expectedSource, hit.getSource().keySet());
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
		SearchHits response = query(String.format("select * from %s/account where city = 'Nogal' LIMIT 1000", TEST_INDEX));
		SearchHit[] hits = response.getHits();

		// assert the results is correct according to accounts.json data.
		Assert.assertEquals(1, response.getTotalHits());
		Assert.assertEquals("Nogal", hits[0].getSource().get("city"));
	}


	// TODO search 'quick fox' still matching 'quick fox brown' this is wrong behavior.
	// in some cases, depends on the analasis, we might want choose better behavior for equallity.
	@Test
	public void equallityTest_phrase() throws SqlParseException, SQLFeatureNotSupportedException {
		SearchHits response = query(String.format("SELECT * FROM %s/phrase WHERE phrase = 'quick fox here' LIMIT 1000", TEST_INDEX));
		SearchHit[] hits = response.getHits();

		// assert the results is correct according to accounts.json data.
		Assert.assertEquals(1, response.getTotalHits());
		Assert.assertEquals("quick fox here", hits[0].getSource().get("phrase"));
	}


	@Test
	public void greaterThanTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		int someAge = 25;
		SearchHits response = query(String.format("SELECT * FROM %s WHERE age > %s LIMIT 1000", TEST_INDEX, someAge));
		SearchHit[] hits = response.getHits();
		for(SearchHit hit : hits) {
			int age = (int) hit.getSource().get("age");
			assertThat(age, greaterThan(someAge));
		}
	}


	@Test
	public void greaterThanOrEqualTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		int someAge = 25;
		SearchHits response = query(String.format("SELECT * FROM %s WHERE age >= %s LIMIT 1000", TEST_INDEX, someAge));
		SearchHit[] hits = response.getHits();

		boolean isEqualFound = false;
		for(SearchHit hit : hits) {
			int age = (int) hit.getSource().get("age");
			assertThat(age, greaterThanOrEqualTo(someAge));

			if(age == someAge)
				isEqualFound = true;
		}

		Assert.assertTrue(String.format("at least one of the documents need to contains age equal to %s", someAge), isEqualFound);
	}


	@Test
	public void lessThanTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		int someAge = 25;
		SearchHits response = query(String.format("SELECT * FROM %s WHERE age < %s LIMIT 1000", TEST_INDEX, someAge));
		SearchHit[] hits = response.getHits();
		for(SearchHit hit : hits) {
			int age = (int) hit.getSource().get("age");
			assertThat(age, lessThan(someAge));
		}
	}


	@Test
	public void lessThanOrEqualTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		int someAge = 25;
		SearchHits response = query(String.format("SELECT * FROM %s WHERE age <= %s LIMIT 1000", TEST_INDEX, someAge));
		SearchHit[] hits = response.getHits();

		boolean isEqualFound = false;
		for(SearchHit hit : hits) {
			int age = (int) hit.getSource().get("age");
			assertThat(age, lessThanOrEqualTo(someAge));

			if(age == someAge)
				isEqualFound = true;
		}

		Assert.assertTrue(String.format("at least one of the documents need to contains age equal to %s", someAge), isEqualFound);
	}


	@Test
	public void orTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		SearchHits response = query(String.format("SELECT * FROM %s/account WHERE gender='F' OR gender='M' LIMIT 1000", TEST_INDEX));
		// Assert all documents from accounts.json is returned.
		Assert.assertEquals(1000, response.getTotalHits());
	}


	@Test
	public void andTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		SearchHits response = query(String.format("SELECT * FROM %s WHERE age=32 AND gender='M' LIMIT 1000", TEST_INDEX));
		SearchHit[] hits = response.getHits();
		for(SearchHit hit : hits) {
			Assert.assertEquals(32, hit.getSource().get("age"));
			Assert.assertEquals("M", hit.getSource().get("gender"));
		}
	}


	@Test
	public void likeTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		SearchHits response = query(String.format("SELECT * FROM %s WHERE firstname LIKE 'amb%%' LIMIT 1000", TEST_INDEX));
		SearchHit[] hits = response.getHits();

		// assert the results is correct according to accounts.json data.
		Assert.assertEquals(1, response.getTotalHits());
		Assert.assertEquals("Amber", hits[0].getSource().get("firstname"));
	}


	@Test
	public void limitTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		SearchHits response = query(String.format("SELECT * FROM %s LIMIT 30", TEST_INDEX));
		SearchHit[] hits = response.getHits();

		// assert the results is correct according to accounts.json data.
		Assert.assertEquals(30, hits.length);
	}


	@Test
	public void betweenTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		int min = 27;
		int max = 30;
		SearchHits response = query(String.format("SELECT * FROM %s WHERE age BETWEEN %s AND %s LIMIT 1000", TEST_INDEX, min, max));
		SearchHit[] hits = response.getHits();
		for(SearchHit hit : hits) {
			int age = (int) hit.getSource().get("age");
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
		SearchHits response = query(String.format("SELECT * FROM %s WHERE age NOT BETWEEN %s AND %s LIMIT 1000", TEST_INDEX, min, max));
		SearchHit[] hits = response.getHits();
		for(SearchHit hit : hits) {
			Map<String, Object> source = hit.getSource();

			// ignore document which not contains the age field.
			if(source.containsKey("age")) {
				int age = (int) hit.getSource().get("age");
				assertThat(age, not(allOf(greaterThanOrEqualTo(min), lessThanOrEqualTo(max))));
			}
		}
	}


	@Test
	public void inTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
		SearchHits response = query(String.format("SELECT age FROM %s/phrase WHERE age IN (20, 22) LIMIT 1000", TEST_INDEX));
		SearchHit[] hits = response.getHits();
		for(SearchHit hit : hits) {
			int age = (int) hit.getSource().get("age");
			assertThat(age, isOneOf(20, 22));
		}
	}


	@Test
	public void inTestWithStrings() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
		SearchHits response = query(String.format("SELECT phrase FROM %s/phrase WHERE phrase IN ('quick fox here', 'fox brown') LIMIT 1000", TEST_INDEX));
		SearchHit[] hits = response.getHits();
		Assert.assertEquals(2, response.getTotalHits());
		for(SearchHit hit : hits) {
			String phrase = (String) hit.getSource().get("phrase");
			assertThat(phrase, isOneOf("quick fox here", "fox brown"));
		}
	}


	/* TODO when using not in on some field, documents that not contains this
	field will return as well, That may considered a Wrong behaivor.
	*/
	@Test
	public void notInTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		SearchHits response = query(String.format("SELECT age FROM %s WHERE age NOT IN (20, 22) LIMIT 1000", TEST_INDEX));
		SearchHit[] hits = response.getHits();
		for(SearchHit hit : hits) {
			Map<String, Object> source = hit.getSource();

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

		SearchHits response = query(String.format("SELECT insert_time FROM %s/online WHERE insert_time < '2014-08-18'", TEST_INDEX));
		SearchHit[] hits = response.getHits();
		for(SearchHit hit : hits) {
			Map<String, Object> source = hit.getSource();
			DateTime insertTime = formatter.parseDateTime((String) source.get("insert_time"));

			String errorMessage = String.format("insert_time must be smaller then 2014-08-18. found: %s", insertTime);
			Assert.assertTrue(errorMessage, insertTime.isBefore(dateToCompare));
		}
	}

//    @Test
//    public void dateSearchBraces() throws IOException, SqlParseException, SQLFeatureNotSupportedException, ParseException {
//        DateTimeFormatter formatter = DateTimeFormat.forPattern(TS_DATE_FORMAT);
//        DateTime dateToCompare = new DateTime(2015, 3, 15, 0, 0, 0);
//
//        SearchHits response = query(String.format("SELECT insert_time FROM %s/odbc WHERE insert_time < {ts '2015-03-15 00:00:00.000'}", TEST_INDEX));
//        SearchHit[] hits = response.getHits();
//        for(SearchHit hit : hits) {
//            Map<String, Object> source = hit.getSource();
//			String insertTimeStr = (String) source.get("insert_time");
//			insertTimeStr = insertTimeStr.replace("{ts '", "").replace("'}", "");
//
//            DateTime insertTime = formatter.parseDateTime(insertTimeStr);
//
//            String errorMessage = String.format("insert_time must be smaller then 2015-03-15. found: %s", insertTime);
//            Assert.assertTrue(errorMessage, insertTime.isBefore(dateToCompare));
//        }
//    }


	@Test
	public void dateBetweenSearch() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		DateTimeFormatter formatter = DateTimeFormat.forPattern(DATE_FORMAT);

		DateTime dateLimit1 = new DateTime(2014, 8, 18, 0, 0, 0);
		DateTime dateLimit2 = new DateTime(2014, 8, 21, 0, 0, 0);

		SearchHits response = query(String.format("SELECT insert_time FROM %s/online WHERE insert_time BETWEEN '2014-08-18' AND '2014-08-21' LIMIT 3", TEST_INDEX));
		SearchHit[] hits = response.getHits();
		for(SearchHit hit : hits) {
			Map<String, Object> source = hit.getSource();
			DateTime insertTime = formatter.parseDateTime((String) source.get("insert_time"));

			boolean isBetween =
					(insertTime.isAfter(dateLimit1) || insertTime.isEqual(dateLimit1)) &&
					(insertTime.isBefore(dateLimit2) || insertTime.isEqual(dateLimit2));

			Assert.assertTrue("insert_time must be between 2014-08-18 and 2014-08-21", isBetween);
		}
	}


	@Test
	public void missFilterSearch() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
		SearchHits response = query(String.format("SELECT * FROM %s/phrase WHERE insert_time IS missing", TEST_INDEX));
		SearchHit[] hits = response.getHits();

		// should be 2 according to the data.
		Assert.assertEquals(response.getTotalHits(), 2);
		for(SearchHit hit : hits) {
			assertThat(hit.getSource(), not(hasKey("insert_time")));
		}
	}

	@Test
	public void notMissFilterSearch() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
		SearchHits response = query(String.format("SELECT * FROM %s/phrase WHERE insert_time IS NOT missing", TEST_INDEX));
		SearchHit[] hits = response.getHits();

		// should be 2 according to the data.
		Assert.assertEquals(response.getTotalHits(), 2);
		for(SearchHit hit : hits) {
			assertThat(hit.getSource(), hasKey("insert_time"));
		}
	}
	


	@Test
	public void complexConditionQuery() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
		String errorMessage = "Result does not exist to the condition (gender='m' AND (age> 25 OR account_number>5)) OR (gender='f' AND (age>30 OR account_number < 8)";

		SearchHits response = query(String.format("SELECT * FROM %s/account WHERE (gender='m' AND (age> 25 OR account_number>5)) OR (gender='f' AND (age>30 OR account_number < 8))", TEST_INDEX));
		SearchHit[] hits = response.getHits();

		for(SearchHit hit : hits) {
			Map<String, Object> source = hit.getSource();
			String gender = ((String)source.get("gender")).toLowerCase();
			int age = (int)source.get("age");
			int account_number = (int) source.get("account_number");

			Assert.assertTrue(errorMessage, (gender.equals("m") && (age> 25 || account_number>5)) || (gender.equals("f") && (age>30 || account_number < 8)));
		}
	}


	@Test
	public void orderByAscTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		SearchHits response = query(String.format("SELECT age FROM %s/account ORDER BY age ASC LIMIT 1000", TEST_INDEX));
		SearchHit[] hits = response.getHits();

		ArrayList<Integer> ages = new ArrayList<Integer>();
		for(SearchHit hit : hits) {
			ages.add((int)hit.getSource().get("age"));
		}

		ArrayList<Integer> sortedAges = (ArrayList<Integer>)ages.clone();
		Collections.sort(sortedAges);
		Assert.assertTrue("The list is not ordered ascending", sortedAges.equals(ages));
	}


	@Test
	public void orderByDescTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		SearchHits response = query(String.format("SELECT age FROM %s/account ORDER BY age DESC LIMIT 1000", TEST_INDEX));
		SearchHit[] hits = response.getHits();

		ArrayList<Integer> ages = new ArrayList<Integer>();
		for(SearchHit hit : hits) {
			ages.add((int)hit.getSource().get("age"));
		}

		ArrayList<Integer> sortedAges = (ArrayList<Integer>)ages.clone();
		Collections.sort(sortedAges, Collections.reverseOrder());
		Assert.assertTrue("The list is not ordered descending", sortedAges.equals(ages));
	}


	@Test
	public void orderByAscFieldWithSpaceTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		SearchHits response = query(String.format("SELECT * FROM %s/phrase_2 ORDER BY `test field` ASC LIMIT 1000", TEST_INDEX));
		SearchHit[] hits = response.getHits();

		ArrayList<Integer> testFields = new ArrayList<Integer>();
		for(SearchHit hit : hits) {
			testFields.add((int)hit.getSource().get("test field"));
		}

		ArrayList<Integer> sortedTestFields = (ArrayList<Integer>)testFields.clone();
		Collections.sort(sortedTestFields);
		Assert.assertTrue("The list is not ordered ascending", sortedTestFields.equals(testFields));
	}

    @Test
    public void testMultipartWhere() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SearchHits response = query(String.format("SELECT * FROM %s/account WHERE (firstname LIKE 'opal' OR firstname like 'rodriquez') AND (state like 'oh' OR state like 'hi')", TEST_INDEX));
        Assert.assertEquals(2, response.getTotalHits());
    }

    @Test
    public void testMultipartWhere2() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SearchHits response = query(String.format("SELECT * FROM %s/account where ((account_number > 200 and account_number < 300) or gender like 'm') and (state like 'hi' or address like 'avenue')", TEST_INDEX));
        Assert.assertEquals(127, response.getTotalHits());
    }

    @Test
    public void testMultipartWhere3() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SearchHits response = query(String.format("SELECT * FROM %s/account where ((account_number > 25 and account_number < 75) and age >35 ) and (state like 'md' or (address like 'avenue' or address like 'street'))", TEST_INDEX));
        Assert.assertEquals(7, response.getTotalHits());
    }

    @Test
    public void filterPolygonTest() throws SQLFeatureNotSupportedException, SqlParseException, InterruptedException {
        SearchHits results = query(String.format("SELECT * FROM %s WHERE GEO_INTERSECTS(place,'POLYGON ((102 2, 103 2, 103 3, 102 3, 102 2))')", TEST_INDEX));
        org.junit.Assert.assertEquals(1,results.getTotalHits());
        SearchHit result = results.getAt(0);
        Assert.assertEquals("bigSquare",result.getSource().get("description"));
    }

    @Test
    public void boundingBox() throws SQLFeatureNotSupportedException, SqlParseException, InterruptedException {
        SearchHits results = query(String.format("SELECT * FROM %s WHERE GEO_BOUNDING_BOX(center,100.0,1.0,101,0.0)", TEST_INDEX));
        org.junit.Assert.assertEquals(1,results.getTotalHits());
        SearchHit result = results.getAt(0);
        Assert.assertEquals("square",result.getSource().get("description"));
    }
    @Test
    public void geoDistance() throws SQLFeatureNotSupportedException, SqlParseException, InterruptedException {
        SearchHits results = query(String.format("SELECT * FROM %s WHERE GEO_DISTANCE(center,'1km',100.5,0.500001)", TEST_INDEX));
        org.junit.Assert.assertEquals(1,results.getTotalHits());
        SearchHit result = results.getAt(0);
        Assert.assertEquals("square",result.getSource().get("description"));
    }

    @Test
    public void geoDistanceRange() throws SQLFeatureNotSupportedException, SqlParseException, InterruptedException {
        SearchHits results = query(String.format("SELECT * FROM %s WHERE GEO_DISTANCE_RANGE(center,'1m','1km',100.5,0.50001)", TEST_INDEX));
        org.junit.Assert.assertEquals(1,results.getTotalHits());
        SearchHit result = results.getAt(0);
        Assert.assertEquals("square",result.getSource().get("description"));
    }

    @Test
    public void geoCell() throws SQLFeatureNotSupportedException, SqlParseException, InterruptedException {
        SearchHits results = query(String.format("SELECT * FROM %s WHERE GEO_CELL(center,100.5,0.50001,7)", TEST_INDEX));
        org.junit.Assert.assertEquals(1,results.getTotalHits());
        SearchHit result = results.getAt(0);
        Assert.assertEquals("square",result.getSource().get("description"));
    }

    @Test
    public void geoPolygon() throws SQLFeatureNotSupportedException, SqlParseException, InterruptedException {
        SearchHits results = query(String.format("SELECT * FROM %s WHERE GEO_POLYGON(center,100,0,100.5,2,101.0,0)", TEST_INDEX));
        org.junit.Assert.assertEquals(1,results.getTotalHits());
        SearchHit result = results.getAt(0);
        Assert.assertEquals("square",result.getSource().get("description"));
    }

    @Test
    public void escapedCharactersCheck() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SearchHits response = query(String.format("SELECT * FROM %s/gotCharacters where nickname = 'Daenerys \"Stormborn\"' LIMIT 1000", TEST_INDEX));
        Assert.assertEquals(1, response.getTotalHits());
    }

    @Test
    public void complexObjectSearch() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SearchHits response = query(String.format("SELECT * FROM %s/gotCharacters where name.firstname = 'Jaime' LIMIT 1000", TEST_INDEX));
        Assert.assertEquals(1, response.getTotalHits());
    }

    @Test
    public void complexObjectReutrnField() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        SearchHits response = query(String.format("SELECT parents.father FROM %s/gotCharacters where name.firstname = 'Brandon' LIMIT 1000", TEST_INDEX));
        Assert.assertEquals(1, response.getTotalHits());
        Map<String, Object> sourceAsMap = response.getHits()[0].sourceAsMap();
        Assert.assertEquals("Eddard",((HashMap<String,Object>)sourceAsMap.get("parents")).get("father"));
    }


    @Test
    public void notLikeTests() throws IOException, SqlParseException, SQLFeatureNotSupportedException{
        //cant use string.format cause of %d
        SearchHits response = query("SELECT name FROM " +TEST_INDEX + "/gotCharacters where name.firstname not like '%d' LIMIT 1000");
        Assert.assertEquals(3, response.getTotalHits());
        for(SearchHit hit : response.getHits()) {
            Map<String, Object> sourceAsMap = hit.sourceAsMap();
            String name = ((HashMap<String, Object>) sourceAsMap.get("name")).get("firstname").toString();
            Assert.assertFalse(name+" was in not like %d",name.startsWith("d"));
        }
    }

    private SearchHits query(String query) throws SqlParseException, SQLFeatureNotSupportedException, SQLFeatureNotSupportedException {
        SearchDao searchDao = MainTestSuite.getSearchDao();
        SqlElasticSearchRequestBuilder select = (SqlElasticSearchRequestBuilder) searchDao.explain(query);
        return ((SearchResponse)select.get()).getHits();
    }
}
