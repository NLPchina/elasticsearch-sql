package org.nlpcn.es4sql;


import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.metrics.scripted.ScriptedMetric;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.junit.Assert;
import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.SqlElasticSearchRequestBuilder;

import java.io.IOException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.*;
import static org.elasticsearch.search.aggregations.bucket.range.Range.Bucket;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX;

public class AggregationTest {

	@Test
	public void countTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		Aggregations result = query(String.format("SELECT COUNT(*) FROM %s/account", TEST_INDEX));
		ValueCount count = result.get("COUNT(*)");
		Assert.assertEquals(1000, count.getValue());
	}

	@Test
	public void sumTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		Aggregations result = query(String.format("SELECT SUM(balance) FROM %s/account", TEST_INDEX));
		Sum sum = result.get("SUM(balance)");
		assertThat(sum.getValue(), equalTo(25714837.0));
	}

    // script on metric aggregation tests. uncomment if your elastic has scripts enable (disabled by default)
    //todo: find a way to check if scripts are enabled
//    @Test
//    public void sumWithScriptTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
//        Aggregations result = query(String.format("SELECT SUM(script('','doc[\\'balance\\'].value + doc[\\'balance\\'].value')) as doubleSum FROM %s/account", TEST_INDEX));
//        Sum sum = result.get("doubleSum");
//        assertThat(sum.getValue(), equalTo(25714837.0*2));
//    }
//
//    @Test
//    public void sumWithImplicitScriptTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
//        Aggregations result = query(String.format("SELECT SUM(balance + balance) as doubleSum FROM %s/account", TEST_INDEX));
//        Sum sum = result.get("doubleSum");
//        assertThat(sum.getValue(), equalTo(25714837.0*2));
//    }
//
//    @Test
//    public void sumWithScriptTestNoAlias() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
//        Aggregations result = query(String.format("SELECT SUM(balance + balance) FROM %s/account", TEST_INDEX));
//        Sum sum = result.get("SUM(script=script(balance + balance,doc('balance').value + doc('balance').value))");
//        assertThat(sum.getValue(), equalTo(25714837.0*2));
//    }
//
//    @Test
//    public void scriptedMetricAggregation() throws SQLFeatureNotSupportedException, SqlParseException {
//        Aggregations result = query ("select scripted_metric('map_script'='if(doc[\\'balance\\'].value > 49670){ if(!_agg.containsKey(\\'ages\\')) { _agg.put(\\'ages\\',doc[\\'age\\'].value); } " +
//                "else { _agg.put(\\'ages\\',_agg.get(\\'ages\\')+doc[\\'age\\'].value); }}'," +
//                "'reduce_script'='sumThem = 0; for (a in _aggs) { if(a.containsKey(\\'ages\\')){ sumThem += a.get(\\'ages\\');} }; return sumThem;') as wierdSum from " + TEST_INDEX + "/account");
//        ScriptedMetric metric = result.get("wierdSum");
//        Assert.assertEquals(136L,metric.aggregation());
//    }
//
//    @Test
//    public void scriptedMetricConcatWithStringParamAndReduceParamAggregation() throws SQLFeatureNotSupportedException, SqlParseException {
//        String query = "select scripted_metric(\n" +
//                "  'init_script' = '_agg[\"concat\"]=[] ',\n" +
//                "  'map_script'='_agg.concat.add(doc[field].value)' ,\n" +
//                "  'combine_script'='return _agg.concat.join(delim);',\t\t\t\t\n" +
//                "  'reduce_script'='_aggs.removeAll(\"\"); return _aggs.join(delim)'," +
//                "'@field' = 'name.firstname' , '@delim'=';',@reduce_delim =';' ) as all_characters \n" +
//                "from "+TEST_INDEX+"/gotCharacters";
//        Aggregations result = query (query);
//        ScriptedMetric metric = result.get("all_characters");
//        List<String> names = Arrays.asList(metric.aggregation().toString().split(";"));
//
//
//        Assert.assertEquals(4,names.size());
//        String[] expectedNames = new String[]{"brandon","daenerys","eddard","jaime"};
//        for(String name : expectedNames){
//            Assert.assertTrue("not contains:" + name,names.contains(name));
//        }
//    }
//
//    @Test
//    public void scriptedMetricAggregationWithNumberParams() throws SQLFeatureNotSupportedException, SqlParseException {
//        Aggregations result = query ("select scripted_metric('map_script'='if(doc[\\'balance\\'].value > 49670){ if(!_agg.containsKey(\\'ages\\')) { _agg.put(\\'ages\\',doc[\\'age\\'].value+x); } " +
//                "else { _agg.put(\\'ages\\',_agg.get(\\'ages\\')+doc[\\'age\\'].value+x); }}'," +
//                "'reduce_script'='sumThem = 0; for (a in _aggs) { if(a.containsKey(\\'ages\\')){ sumThem += a.get(\\'ages\\');} }; return sumThem;'" +
//                ",'@x'=3) as wierdSum from " + TEST_INDEX + "/account");
//        ScriptedMetric metric = result.get("wierdSum");
//        Assert.assertEquals(148L,metric.aggregation());
//    }
//

    @Test
	public void minTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		Aggregations result = query(String.format("SELECT MIN(age) FROM %s/account", TEST_INDEX));
		Min min = result.get("MIN(age)");
		assertThat(min.getValue(), equalTo(20.0));
	}

	@Test
	public void maxTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		Aggregations result = query(String.format("SELECT MAX(age) FROM %s/account", TEST_INDEX));
		Max max = result.get("MAX(age)");
		assertThat(max.getValue(), equalTo(40.0));
	}

	@Test
	public void avgTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		Aggregations result = query(String.format("SELECT AVG(age) FROM %s/account", TEST_INDEX));
		Avg avg = result.get("AVG(age)");
		assertThat(avg.getValue(), equalTo(30.171));
	}

	@Test
	public void statsTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		Aggregations result = query(String.format("SELECT STATS(age) FROM %s/account", TEST_INDEX));
		Stats stats = result.get("STATS(age)");
		Assert.assertEquals(1000, stats.getCount());
		assertThat(stats.getSum(), equalTo(30171.0));
		assertThat(stats.getMin(), equalTo(20.0));
		assertThat(stats.getMax(), equalTo(40.0));
		assertThat(stats.getAvg(), equalTo(30.171));
	}

    @Test
    public void extendedStatsTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
        Aggregations result = query(String.format("SELECT EXTENDED_STATS(age) FROM %s/account", TEST_INDEX));
        ExtendedStats stats = result.get("EXTENDED_STATS(age)");
        Assert.assertEquals(1000, stats.getCount());
        assertThat(stats.getMin(),equalTo(20.0));
        assertThat(stats.getMax(),equalTo(40.0));
        assertThat(stats.getAvg(),equalTo(30.171));
        assertThat(stats.getSum(),equalTo(30171.0));
        assertThat(stats.getSumOfSquares(),equalTo(946393.0));
        Assert.assertTrue(Math.abs(stats.getStdDeviation()- 6.008640362012022) < 0.0001);
        Assert.assertTrue(Math.abs(stats.getVariance()- 36.10375899999996) < 0.0001);
    }

    @Test
    public void percentileTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
        Aggregations result = query(String.format("SELECT PERCENTILES(age) FROM %s/account", TEST_INDEX));
        Percentiles percentiles = result.get("PERCENTILES(age)");
        Assert.assertTrue(Math.abs(percentiles.percentile(1.0) - 20.0) < 0.001 );
        Assert.assertTrue(Math.abs(percentiles.percentile(5.0) - 21.0) < 0.001 );
        Assert.assertTrue(Math.abs(percentiles.percentile(25.0) - 25.0) < 0.001 );
        Assert.assertTrue(Math.abs(percentiles.percentile(75.0) - 35.0) < 0.001 );
        Assert.assertTrue(Math.abs(percentiles.percentile(95.0) - 39.0) < 0.001 );
        Assert.assertTrue(Math.abs(percentiles.percentile(99.0) - 40.0) < 0.001 );
    }

    @Test
	public void aliasTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		Aggregations result = query(String.format("SELECT COUNT(*) AS mycount FROM %s/account", TEST_INDEX));
		assertThat(result.asMap(), hasKey("mycount"));
	}

	@Test
	public void groupByTest() throws Exception {
		Aggregations result = query(String.format("SELECT COUNT(*) FROM %s/account GROUP BY gender", TEST_INDEX));
		Terms gender = result.get("gender");
		for(Terms.Bucket bucket : gender.getBuckets()) {
			String key = bucket.getKey();
			long count = ((ValueCount) bucket.getAggregations().get("COUNT(*)")).getValue();
			if(key.equalsIgnoreCase("m")) {
				Assert.assertEquals(507, count);
			}
			else if(key.equalsIgnoreCase("f")) {
				Assert.assertEquals(493, count);
			}
			else {
				throw new Exception(String.format("Unexpected key. expected: m OR f. found: %s", key));
			}
		}
	}

	@Test
	public void multipleGroupByTest() throws Exception {
		Set expectedAges = new HashSet<Integer>(ContiguousSet.create(Range.closed(20, 40), DiscreteDomain.integers()));

		Map<String, Set<Integer>> buckets = new HashMap<>();

		Aggregations result = query(String.format("SELECT COUNT(*) FROM %s/account GROUP BY gender, age", TEST_INDEX));
		Terms gender = result.get("gender");
		for(Terms.Bucket genderBucket : gender.getBuckets()) {
			String genderKey = genderBucket.getKey();
			buckets.put(genderKey, new HashSet<Integer>());
			Terms ageBuckets = (Terms) genderBucket.getAggregations().get("age");
			for(Terms.Bucket ageBucket : ageBuckets.getBuckets()) {
				buckets.get(genderKey).add(Integer.parseInt(ageBucket.getKey()));
			}
		}

		Assert.assertEquals(2, buckets.keySet().size());
		Assert.assertEquals(expectedAges, buckets.get("m"));
		Assert.assertEquals(expectedAges, buckets.get("f"));
	}


	@Test
	public void orderByAscTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		ArrayList<Long> agesCount = new ArrayList<>();

		Aggregations result = query(String.format("SELECT COUNT(*) FROM %s/account GROUP BY age ORDER BY COUNT(*)", TEST_INDEX));
		Terms age = result.get("age");

		for(Terms.Bucket bucket : age.getBuckets()) {
			agesCount.add(((ValueCount) bucket.getAggregations().get("COUNT(*)")).getValue());
		}

		ArrayList<Long> sortedAgesCount = (ArrayList<Long>)agesCount.clone();
		Collections.sort(sortedAgesCount);
		Assert.assertTrue("The list is not ordered ascending", agesCount.equals(agesCount));
	}


	@Test
	public void orderByDescTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		ArrayList<Long> agesCount = new ArrayList<>();

		Aggregations result = query(String.format("SELECT COUNT(*) FROM %s/account GROUP BY age ORDER BY COUNT(*) DESC", TEST_INDEX));
		Terms age = result.get("age");

		for(Terms.Bucket bucket : age.getBuckets()) {
			agesCount.add(((ValueCount) bucket.getAggregations().get("COUNT(*)")).getValue());
		}

		ArrayList<Long> sortedAgesCount = (ArrayList<Long>)agesCount.clone();
		Collections.sort(sortedAgesCount, Collections.reverseOrder());
		Assert.assertTrue("The list is not ordered descending", agesCount.equals(agesCount));
	}

	@Test
	public void limitTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		Aggregations result = query(String.format("SELECT COUNT(*) FROM %s/account GROUP BY age ORDER BY COUNT(*) LIMIT 5", TEST_INDEX));
		Terms age = result.get("age");

		assertThat(age.getBuckets().size(), equalTo(5));
	}

	@Test
	public void countGroupByRange() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		Aggregations result = query(String.format("SELECT COUNT(age) FROM %s/account GROUP BY range(age, 20,25,30,35,40) ", TEST_INDEX));
		org.elasticsearch.search.aggregations.bucket.range.Range  ageRanges = result.get("range(age,20,25,30,35,40)");
		assertThat(ageRanges.getBuckets().size(), equalTo(4));

		long[] expectedResults = new long[] {225L, 226L, 259L, 245L};
		int index = 0;
		for(Bucket bucket : ageRanges.getBuckets()) {
			assertThat(((ValueCount) bucket.getAggregations().get("COUNT(age)")).getValue(), equalTo(expectedResults[index]));
			index++;
		}
	}

	/**
	 * 时间 聚合 , 每天按照天聚合 参数说明:
	 *
	 * <a>http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-aggregations-bucket-datehistogram-aggregation.html</a>
	 *
	 * @throws IOException
	 * @throws SqlParseException
	 */
	@Test
	public void countGroupByDateTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
        SqlElasticSearchRequestBuilder result = (SqlElasticSearchRequestBuilder) MainTestSuite.getSearchDao().explain("select insert_time from online  group by date_histogram(field='insert_time','interval'='1.5h','format'='yyyy-MM') ");
		System.out.println(result);
	}

    @Test
    public void countGroupByDateTestWithAlias() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
        SqlElasticSearchRequestBuilder result = (SqlElasticSearchRequestBuilder) MainTestSuite.getSearchDao().explain("select insert_time from online  group by date_histogram(field='insert_time','interval'='1.5h','format'='yyyy-MM','alias'='myAlias') ");
        boolean containAlias = result.toString().replaceAll("\\s+","").contains("myAlias\":{\"date_histogram\":{\"field\":\"insert_time\",\"interval\":\"1.5h\",\"format\":\"yyyy-MM\"}}");
        Assert.assertTrue(containAlias);
    }

	/**
	 * 时间范围聚合
	 *
	 * <a>http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-aggregations-bucket-daterange-aggregation.html</a>
	 *
	 * @throws IOException
	 * @throws SqlParseException
	 */
	@Test
	public void countDateRangeTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
        SqlElasticSearchRequestBuilder result = (SqlElasticSearchRequestBuilder) MainTestSuite.getSearchDao().explain("select online from online  group by date_range(field='insert_time','format'='yyyy-MM-dd' ,'2014-08-18','2014-08-17','now-8d','now-7d','now-6d','now') ");
		System.out.println(result);
	}


	/**
	 * tophits 查询
	 *
	 * <a>http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-aggregations-metrics-top-hits-aggregation.html</a>
	 *
	 * @throws IOException
	 * @throws SqlParseException
	 */
	@Test
	public void topHitTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
        SqlElasticSearchRequestBuilder result = (SqlElasticSearchRequestBuilder) MainTestSuite.getSearchDao().explain("select topHits('size'=3,age='desc') from bank  group by gender ");
		System.out.println(result);
	}


	private Aggregations query(String query) throws SqlParseException, SQLFeatureNotSupportedException {
		SearchDao searchDao = MainTestSuite.getSearchDao();
        SqlElasticSearchRequestBuilder select = (SqlElasticSearchRequestBuilder) searchDao.explain(query);
		return ((SearchResponse)select.get()).getAggregations();
	}


	@Test
	public void testSubAggregations() throws  Exception {
		Set expectedAges = new HashSet<>(ContiguousSet.create(Range.closed(20, 40), DiscreteDomain.integers()));
		final String query = String.format("SELECT * FROM %s/account GROUP BY (gender, age), (state) LIMIT 0,10", TEST_INDEX);

		Map<String, Set<Integer>> buckets = new HashMap<>();

		SearchDao searchDao = MainTestSuite.getSearchDao();
        SqlElasticSearchRequestBuilder select = (SqlElasticSearchRequestBuilder) searchDao.explain(query);
		SearchResponse response = (SearchResponse) select.get();
		Aggregations result = response.getAggregations();

		Terms gender = result.get("gender");
		for(Terms.Bucket genderBucket : gender.getBuckets()) {
			String genderKey = genderBucket.getKey();
			buckets.put(genderKey, new HashSet<Integer>());
			Terms ageBuckets = (Terms) genderBucket.getAggregations().get("age");
			for(Terms.Bucket ageBucket : ageBuckets.getBuckets()) {
				buckets.get(genderKey).add(Integer.parseInt(ageBucket.getKey()));
			}
		}

		Assert.assertEquals(2, buckets.keySet().size());
		Assert.assertEquals(expectedAges, buckets.get("m"));
		Assert.assertEquals(expectedAges, buckets.get("f"));

		Terms state = result.get("state");
		for(Terms.Bucket stateBucket : state.getBuckets()) {
			if(stateBucket.getKey().equalsIgnoreCase("ak")) {
				Assert.assertTrue("There are 22 entries for state ak", stateBucket.getDocCount() == 22);
			}
		}

		Assert.assertEquals(response.getHits().totalHits(), 1000);
		Assert.assertEquals(response.getHits().hits().length, 10);
	}

	@Test
	public void testSimpleSubAggregations() throws  Exception {
		final String query = String.format("SELECT * FROM %s/account GROUP BY (gender), (state) LIMIT 0,10", TEST_INDEX);

		SearchDao searchDao = MainTestSuite.getSearchDao();
		SqlElasticSearchRequestBuilder select = (SqlElasticSearchRequestBuilder) searchDao.explain(query);
		SearchResponse response = (SearchResponse) select.get();
		Aggregations result = response.getAggregations();

		Terms gender = result.get("gender");
		for(Terms.Bucket genderBucket : gender.getBuckets()) {
			String genderKey = genderBucket.getKey();
			Assert.assertTrue("Gender should be m or f", genderKey.equals("m") || genderKey.equals("f"));
		}

		Assert.assertEquals(2, gender.getBuckets().size());

		Terms state = result.get("state");
		for(Terms.Bucket stateBucket : state.getBuckets()) {
			if(stateBucket.getKey().equalsIgnoreCase("ak")) {
				Assert.assertTrue("There are 22 entries for state ak", stateBucket.getDocCount() == 22);
			}
		}

		Assert.assertEquals(response.getHits().totalHits(), 1000);
		Assert.assertEquals(response.getHits().hits().length, 10);
	}

    @Test
    public void geoHashGrid() throws SQLFeatureNotSupportedException, SqlParseException {
        Aggregations result = query(String.format("SELECT COUNT(*) FROM %s/location GROUP BY geohash_grid(field='center',precision=5) ", TEST_INDEX));
        InternalGeoHashGrid grid = result.get("geohash_grid(field=center,precision=5)");
        Collection<GeoHashGrid.Bucket> buckets = grid.getBuckets();
        for (GeoHashGrid.Bucket bucket : buckets) {
            Assert.assertTrue(bucket.getKey().equals("w2fsm") || bucket.getKey().equals("w0p6y") );
            Assert.assertEquals(1,bucket.getDocCount());
        }
    }

}
