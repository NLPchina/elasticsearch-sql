package org.nlpcn.es4sql;


import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilters;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.bucket.nested.InternalNested;
import org.elasticsearch.search.aggregations.bucket.nested.InternalReverseNested;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.aggregations.metrics.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.InternalGeoBounds;
import org.elasticsearch.search.aggregations.metrics.InternalSum;
import org.elasticsearch.search.aggregations.metrics.InternalTopHits;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.aggregations.metrics.Percentiles;
import org.elasticsearch.search.aggregations.metrics.Stats;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.metrics.ValueCount;
import org.junit.Assert;
import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.SqlElasticSearchRequestBuilder;

import java.io.IOException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.search.aggregations.bucket.range.Range.Bucket;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_DOG;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_GAME_OF_THRONES;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_LOCATION;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_NESTED_TYPE;

public class AggregationTest {

	@Test
	public void countTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		Aggregations result = query(String.format("SELECT COUNT(*) FROM %s/account", TEST_INDEX_ACCOUNT));
		ValueCount count = result.get("COUNT(*)");
		Assert.assertEquals(1000, count.getValue());
	}

	@Test
	public void sumTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		Aggregations result = query(String.format("SELECT SUM(balance) FROM %s/account", TEST_INDEX_ACCOUNT));
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
		Aggregations result = query(String.format("SELECT MIN(age) FROM %s/account", TEST_INDEX_ACCOUNT));
		Min min = result.get("MIN(age)");
		assertThat(min.getValue(), equalTo(20.0));
	}

	@Test
	public void maxTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		Aggregations result = query(String.format("SELECT MAX(age) FROM %s/account", TEST_INDEX_ACCOUNT));
		Max max = result.get("MAX(age)");
		assertThat(max.getValue(), equalTo(40.0));
	}

	@Test
	public void avgTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		Aggregations result = query(String.format("SELECT AVG(age) FROM %s/account", TEST_INDEX_ACCOUNT));
		Avg avg = result.get("AVG(age)");
		assertThat(avg.getValue(), equalTo(30.171));
	}

	@Test
	public void statsTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		Aggregations result = query(String.format("SELECT STATS(age) FROM %s/account", TEST_INDEX_ACCOUNT));
		Stats stats = result.get("STATS(age)");
		Assert.assertEquals(1000, stats.getCount());
		assertThat(stats.getSum(), equalTo(30171.0));
		assertThat(stats.getMin(), equalTo(20.0));
		assertThat(stats.getMax(), equalTo(40.0));
		assertThat(stats.getAvg(), equalTo(30.171));
	}

    @Test
    public void extendedStatsTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
        Aggregations result = query(String.format("SELECT EXTENDED_STATS(age) FROM %s/account", TEST_INDEX_ACCOUNT));
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
        Aggregations result = query(String.format("SELECT PERCENTILES(age) FROM %s/account", TEST_INDEX_ACCOUNT));
        Percentiles percentiles = result.get("PERCENTILES(age)");
        Assert.assertTrue(Math.abs(percentiles.percentile(1.0) - 20.0) < 0.001 );
        Assert.assertTrue(Math.abs(percentiles.percentile(5.0) - 21.0) < 0.001 );
        Assert.assertTrue(Math.abs(percentiles.percentile(25.0) - 25.0) < 0.001 );
        Assert.assertTrue(Math.abs(percentiles.percentile(75.0) - 35.0) < 0.001 );
        Assert.assertTrue(Math.abs(percentiles.percentile(95.0) - 39.0) < 0.001 );
        Assert.assertTrue(Math.abs(percentiles.percentile(99.0) - 40.0) < 0.001 );
    }

    @Test
    public void percentileTestSpecific() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
        Aggregations result = query(String.format("SELECT PERCENTILES(age,25.0,75.0) x FROM %s/account", TEST_INDEX_ACCOUNT));
        Percentiles percentiles = result.get("x");
        Assert.assertTrue(Math.abs(percentiles.percentile(25.0) - 25.0) < 0.001 );
        Assert.assertTrue(Math.abs(percentiles.percentile(75.0) - 35.0) < 0.001 );
    }

    @Test
	public void aliasTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		Aggregations result = query(String.format("SELECT COUNT(*) AS mycount FROM %s/account", TEST_INDEX_ACCOUNT));
		assertThat(result.asMap(), hasKey("mycount"));
	}

	@Test
	public void groupByTest() throws Exception {
		Aggregations result = query(String.format("SELECT COUNT(*) FROM %s/account GROUP BY gender", TEST_INDEX_ACCOUNT));
		Terms gender = result.get("gender");
		for(Terms.Bucket bucket : gender.getBuckets()) {
			String key = bucket.getKey().toString();
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
    public void postFilterTest() throws Exception {
        SqlElasticSearchRequestBuilder select = getSearchRequestBuilder(String.format("SELECT /*! POST_FILTER({\"term\":{\"gender\":\"m\"}}) */ COUNT(*) FROM %s/account GROUP BY gender", TEST_INDEX_ACCOUNT));
        SearchResponse res = (SearchResponse) select.get();
        Assert.assertEquals(507, res.getHits().getTotalHits().value);

        Aggregations result = res.getAggregations();
        Terms gender = result.get("gender");
        for (Terms.Bucket bucket : gender.getBuckets()) {
            String key = bucket.getKey().toString();
            long count = ((ValueCount) bucket.getAggregations().get("COUNT(*)")).getValue();
            if (key.equalsIgnoreCase("m")) {
                Assert.assertEquals(507, count);
            } else if (key.equalsIgnoreCase("f")) {
                Assert.assertEquals(493, count);
            } else {
                throw new Exception(String.format("Unexpected key. expected: m OR f. found: %s", key));
            }
        }
    }

	@Test
	public void multipleGroupByTest() throws Exception {
		Set expectedAges = new HashSet<Integer>(ContiguousSet.create(Range.closed(20, 40), DiscreteDomain.integers()));

		Map<String, Set<Integer>> buckets = new HashMap<>();

		Aggregations result = query(String.format("SELECT COUNT(*) FROM %s/account GROUP BY gender,  terms('field'='age','size'=200,'alias'='age')", TEST_INDEX_ACCOUNT));
		Terms gender = result.get("gender");
		for(Terms.Bucket genderBucket : gender.getBuckets()) {
			String genderKey = genderBucket.getKey().toString();
			buckets.put(genderKey, new HashSet<Integer>());
			Terms ageBuckets = (Terms) genderBucket.getAggregations().get("age");
			for(Terms.Bucket ageBucket : ageBuckets.getBuckets()) {
				buckets.get(genderKey).add(Integer.parseInt(ageBucket.getKey().toString()));
			}
		}

		Assert.assertEquals(2, buckets.keySet().size());
		Assert.assertEquals(expectedAges, buckets.get("m"));
		Assert.assertEquals(expectedAges, buckets.get("f"));
	}

    @Test
    public void multipleGroupBysWithSize() throws Exception {
        Set expectedAges = new HashSet<Integer>(ContiguousSet.create(Range.closed(20, 40), DiscreteDomain.integers()));

        Map<String, Set<Integer>> buckets = new HashMap<>();

        Aggregations result = query(String.format("SELECT COUNT(*) FROM %s/account GROUP BY gender, terms('alias'='ageAgg','field'='age','size'=3)", TEST_INDEX_ACCOUNT));
        Terms gender = result.get("gender");
        Assert.assertEquals(2,gender.getBuckets().size());
        for(Terms.Bucket genderBucket : gender.getBuckets()) {

            String genderKey = genderBucket.getKey().toString();
            buckets.put(genderKey, new HashSet<Integer>());
            Terms ageBuckets = genderBucket.getAggregations().get("ageAgg");
            Assert.assertEquals(3,ageBuckets.getBuckets().size());

        }


    }

    @Test
    public void termsWithSize() throws Exception {

        Map<String, Set<Integer>> buckets = new HashMap<>();

        Aggregations result = query(String.format("SELECT COUNT(*) FROM %s/account GROUP BY terms('alias'='ageAgg','field'='age','size'=3)", TEST_INDEX_ACCOUNT));
        Terms gender = result.get("ageAgg");
        Assert.assertEquals(3,gender.getBuckets().size());

    }

    @Test
    public void termsWithMissing() throws Exception {
        Aggregations result = query(String.format("SELECT count(*) FROM %s/gotCharacters GROUP BY terms('alias'='nick','field'='nickname','missing'='no_nickname')", TEST_INDEX_GAME_OF_THRONES));
        Terms name = result.get("nick");
        Assert.assertNotNull(name.getBucketByKey("no_nickname"));
        Assert.assertEquals(6, name.getBucketByKey("no_nickname").getDocCount());
    }
    
    @Test
    public void termsWithOrder() throws Exception {
        Aggregations result = query(String.format("SELECT count(*) FROM %s/dog GROUP BY terms('field'='dog_name', 'alias'='dog_name', order='desc')", TEST_INDEX_DOG));
        Terms name = result.get("dog_name");
        Assert.assertEquals("snoopy",name.getBuckets().get(0).getKeyAsString());
        Assert.assertEquals("rex",name.getBuckets().get(1).getKeyAsString());
        
        result = query(String.format("SELECT count(*) FROM %s/dog GROUP BY terms('field'='dog_name', 'alias'='dog_name', order='asc')", TEST_INDEX_DOG));
        name = result.get("dog_name");        
        Assert.assertEquals("rex",name.getBuckets().get(0).getKeyAsString());
        Assert.assertEquals("snoopy",name.getBuckets().get(1).getKeyAsString());
    }

    @Test
	public void orderByAscTest() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		ArrayList<Long> agesCount = new ArrayList<>();

		Aggregations result = query(String.format("SELECT COUNT(*) FROM %s/account GROUP BY age ORDER BY COUNT(*)", TEST_INDEX_ACCOUNT));
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

		Aggregations result = query(String.format("SELECT COUNT(*) FROM %s/account GROUP BY age ORDER BY COUNT(*) DESC", TEST_INDEX_ACCOUNT));
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
		Aggregations result = query(String.format("SELECT COUNT(*) FROM %s/account GROUP BY age ORDER BY COUNT(*) LIMIT 5", TEST_INDEX_ACCOUNT));
		Terms age = result.get("age");

		assertThat(age.getBuckets().size(), equalTo(5));
	}

	@Test
	public void countGroupByRange() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
		Aggregations result = query(String.format("SELECT COUNT(age) FROM %s/account GROUP BY range(age, 20,25,30,35,40) ", TEST_INDEX_ACCOUNT));
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
        String result = MainTestSuite.getSearchDao().explain("select insert_time from online  group by date_histogram(field='insert_time','interval'='1.5h','format'='yyyy-MM','min_doc_count'=5,'offset'='+8h') ").explain().toString();
        Assert.assertTrue(result.replaceAll("\\s+", "").contains("{\"date_histogram\":{\"field\":\"insert_time\",\"format\":\"yyyy-MM\",\"interval\":\"1.5h\",\"offset\":28800000,\"order\":{\"_key\":\"asc\"},\"keyed\":false,\"min_doc_count\":5}"));
	}

    @Test
    public void countGroupByDateTestWithAlias() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
        SqlElasticSearchRequestBuilder result = (SqlElasticSearchRequestBuilder) MainTestSuite.getSearchDao().explain("select insert_time from online  group by date_histogram(field='insert_time','interval'='1.5h','format'='yyyy-MM','alias'='myAlias') ").explain();
        boolean containAlias = result.toString().replaceAll("\\s+","").contains("myAlias\":{\"date_histogram\":{\"field\":\"insert_time\",\"format\":\"yyyy-MM\",\"interval\":\"1.5h\"");
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
        SqlElasticSearchRequestBuilder result = (SqlElasticSearchRequestBuilder) MainTestSuite.getSearchDao().explain("select online from online  group by date_range(field='insert_time','format'='yyyy-MM-dd' ,'2014-08-18','2014-08-17','now-8d','now-7d','now-6d','now') ").explain();
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
        Aggregations result = query(String.format("select topHits('size'=3,age='desc') from %s group by gender ", TEST_INDEX_ACCOUNT));
		System.out.println(result);
	}


    @Test
    public void topHitTest_WithInclude() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
        Aggregations result = query(String.format("select topHits('size'=3,age='desc',include=age) from %s/account group by gender ", TEST_INDEX_ACCOUNT));
        List<? extends Terms.Bucket> buckets = ((Terms) (result.asList().get(0))).getBuckets();
        for (Terms.Bucket bucket : buckets){
            SearchHits hits = ((InternalTopHits) bucket.getAggregations().asList().get(0)).getHits();
            for(SearchHit hit: hits ){
                Set<String> fields = hit.getSourceAsMap().keySet();
                Assert.assertEquals(1,fields.size());
                Assert.assertEquals("age",fields.toArray()[0]);
            }
        }
    }

    @Test
    public void topHitTest_WithIncludeTwoFields() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
        Aggregations result = query(String.format("select topHits('size'=3,'include'='age,firstname',age='desc') from %s/account group by gender ", TEST_INDEX_ACCOUNT));
        List<? extends Terms.Bucket> buckets = ((Terms) (result.asList().get(0))).getBuckets();
        for (Terms.Bucket bucket : buckets){
            SearchHits hits = ((InternalTopHits) bucket.getAggregations().asList().get(0)).getHits();
            for(SearchHit hit: hits ){
                Set<String> fields = hit.getSourceAsMap().keySet();
                Assert.assertEquals(2,fields.size());
                Assert.assertTrue(fields.contains("age"));
                Assert.assertTrue(fields.contains("firstname"));
            }
        }
    }

    @Test
    public void topHitTest_WithExclude() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
        Aggregations result = query(String.format("select topHits('size'=3,'exclude'='lastname',age='desc') from %s/account group by gender ", TEST_INDEX_ACCOUNT));
        List<? extends Terms.Bucket> buckets = ((Terms) (result.asList().get(0))).getBuckets();
        for (Terms.Bucket bucket : buckets){
            SearchHits hits = ((InternalTopHits) bucket.getAggregations().asList().get(0)).getHits();
            for(SearchHit hit: hits ){
                Set<String> fields = hit.getSourceAsMap().keySet();
                Assert.assertTrue(!fields.contains("lastname"));
            }
        }
    }

    @Test
    public void topHitTest_WithIncludeAndExclude() throws IOException, SqlParseException, SQLFeatureNotSupportedException {
        Aggregations result = query(String.format("select topHits('size'=3,'exclude'='lastname','include'='firstname,lastname',age='desc') from %s/account group by gender ", TEST_INDEX_ACCOUNT));
        List<? extends Terms.Bucket> buckets = ((Terms) (result.asList().get(0))).getBuckets();
        for (Terms.Bucket bucket : buckets) {
            SearchHits hits = ((InternalTopHits) bucket.getAggregations().asList().get(0)).getHits();
            for (SearchHit hit : hits) {
                Set<String> fields = hit.getSourceAsMap().keySet();
                Assert.assertEquals(1, fields.size());
                Assert.assertTrue(fields.contains("firstname"));
            }
        }
    }

	private Aggregations query(String query) throws SqlParseException, SQLFeatureNotSupportedException {
        SqlElasticSearchRequestBuilder select = getSearchRequestBuilder(query);
		return ((SearchResponse)select.get()).getAggregations();
	}

    private SqlElasticSearchRequestBuilder getSearchRequestBuilder(String query) throws SqlParseException, SQLFeatureNotSupportedException {
        SearchDao searchDao = MainTestSuite.getSearchDao();
        return (SqlElasticSearchRequestBuilder) searchDao.explain(query).explain();
    }

    @Test
    public void testFromSizeWithAggregations() throws Exception {
        final String query1 = String.format("SELECT /*! DOCS_WITH_AGGREGATION(0,1) */" +
                " account_number FROM %s/account GROUP BY gender", TEST_INDEX_ACCOUNT);
        SearchResponse response1 = (SearchResponse) getSearchRequestBuilder(query1).get();

        Assert.assertEquals(1, response1.getHits().getHits().length);
        Terms gender1 = response1.getAggregations().get("gender");
        Assert.assertEquals(2, gender1.getBuckets().size());
        Object account1 = response1.getHits().getHits()[0].getSourceAsMap().get("account_number");

        final String query2 = String.format("SELECT /*! DOCS_WITH_AGGREGATION(1,1) */" +
                " account_number FROM %s/account GROUP BY gender", TEST_INDEX_ACCOUNT);
        SearchResponse response2 = (SearchResponse) getSearchRequestBuilder(query2).get();

        Assert.assertEquals(1, response2.getHits().getHits().length);
        Terms gender2 = response2.getAggregations().get("gender");
        Assert.assertEquals(2, gender2.getBuckets().size());
        Object account2 = response2.getHits().getHits()[0].getSourceAsMap().get("account_number");

        Assert.assertEquals(response1.getHits().getTotalHits(), response2.getHits().getTotalHits());
        Assert.assertNotEquals(account1, account2);
    }

    @Test
	public void testSubAggregations() throws  Exception {
		Set expectedAges = new HashSet<>(ContiguousSet.create(Range.closed(20, 40), DiscreteDomain.integers()));
		final String query = String.format("SELECT /*! DOCS_WITH_AGGREGATION(10) */" +
                " * FROM %s/account GROUP BY (gender, terms('field'='age','size'=200,'alias'='age')), (state) LIMIT 200,200", TEST_INDEX_ACCOUNT);

		Map<String, Set<Integer>> buckets = new HashMap<>();

        SqlElasticSearchRequestBuilder select = getSearchRequestBuilder(query);
		SearchResponse response = (SearchResponse) select.get();
		Aggregations result = response.getAggregations();

		Terms gender = result.get("gender");
		for(Terms.Bucket genderBucket : gender.getBuckets()) {
			String genderKey = genderBucket.getKey().toString();
			buckets.put(genderKey, new HashSet<Integer>());
			Terms ageBuckets = (Terms) genderBucket.getAggregations().get("age");
			for(Terms.Bucket ageBucket : ageBuckets.getBuckets()) {
				buckets.get(genderKey).add(Integer.parseInt(ageBucket.getKey().toString()));
			}
		}

		Assert.assertEquals(2, buckets.keySet().size());
		Assert.assertEquals(expectedAges, buckets.get("m"));
		Assert.assertEquals(expectedAges, buckets.get("f"));

		Terms state = result.get("state");
		for(Terms.Bucket stateBucket : state.getBuckets()) {
			if(stateBucket.getKey().toString().equalsIgnoreCase("ak")) {
				Assert.assertTrue("There are 22 entries for state ak", stateBucket.getDocCount() == 22);
			}
		}

		Assert.assertEquals(response.getHits().getTotalHits().value, 1000);
		Assert.assertEquals(response.getHits().getHits().length, 10);
	}

	@Test
	public void testSimpleSubAggregations() throws  Exception {
		final String query = String.format("SELECT /*! DOCS_WITH_AGGREGATION(10) */ * FROM %s/account GROUP BY (gender), (state) ", TEST_INDEX_ACCOUNT);

        SqlElasticSearchRequestBuilder select = getSearchRequestBuilder(query);
		SearchResponse response = (SearchResponse) select.get();
		Aggregations result = response.getAggregations();

		Terms gender = result.get("gender");
		for(Terms.Bucket genderBucket : gender.getBuckets()) {
			String genderKey = genderBucket.getKey().toString();
			Assert.assertTrue("Gender should be m or f", genderKey.equals("m") || genderKey.equals("f"));
		}

		Assert.assertEquals(2, gender.getBuckets().size());

		Terms state = result.get("state");
		for(Terms.Bucket stateBucket : state.getBuckets()) {
			if(stateBucket.getKey().toString().equalsIgnoreCase("ak")) {
				Assert.assertTrue("There are 22 entries for state ak", stateBucket.getDocCount() == 22);
			}
		}

		Assert.assertEquals(response.getHits().getTotalHits().value, 1000);
		Assert.assertEquals(response.getHits().getHits().length, 10);
	}

    @Test
    public void geoHashGrid() throws SQLFeatureNotSupportedException, SqlParseException {
        Aggregations result = query(String.format("SELECT COUNT(*) FROM %s/location GROUP BY geohash_grid(field='center',precision=5) ", TEST_INDEX_LOCATION));
        InternalGeoHashGrid grid = result.get("geohash_grid(field=center,precision=5)");
        Collection<? extends InternalMultiBucketAggregation.InternalBucket> buckets = grid.getBuckets();
        for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {
            Assert.assertTrue(bucket.getKeyAsString().equals("w2fsm") || bucket.getKeyAsString().equals("w0p6y") );
            Assert.assertEquals(1,bucket.getDocCount());
        }
    }

    @Test
    public void geoBounds() throws SQLFeatureNotSupportedException, SqlParseException {
        Aggregations result = query(String.format("SELECT * FROM %s/location GROUP BY geo_bounds(field='center',alias='bounds') ", TEST_INDEX_LOCATION));
        InternalGeoBounds bounds = result.get("bounds");
        Assert.assertEquals(0.5,bounds.bottomRight().getLat(),0.001);
        Assert.assertEquals(105.0,bounds.bottomRight().getLon(),0.001);
        Assert.assertEquals(5.0,bounds.topLeft().getLat(),0.001);
        Assert.assertEquals(100.5,bounds.topLeft().getLon(),0.001);
    }

    @Test
    public void groupByOnNestedFieldTest() throws Exception {
        Aggregations result = query(String.format("SELECT COUNT(*) FROM %s/nestedType GROUP BY nested(message.info)", TEST_INDEX_NESTED_TYPE));
        InternalNested nested = result.get("message.info@NESTED");
        Terms infos = nested.getAggregations().get("message.info");
        Assert.assertEquals(3,infos.getBuckets().size());
        for(Terms.Bucket bucket : infos.getBuckets()) {
            String key = bucket.getKey().toString();
            long count = ((ValueCount) bucket.getAggregations().get("COUNT(*)")).getValue();
            if(key.equalsIgnoreCase("a")) {
                Assert.assertEquals(2, count);
            }
            else if(key.equalsIgnoreCase("c")) {
                Assert.assertEquals(2, count);
            }
            else if(key.equalsIgnoreCase("b")) {
                Assert.assertEquals(1, count);
            }
            else {
                throw new Exception(String.format("Unexpected key. expected: a OR b OR c . found: %s", key));
            }
        }
    }

    @Test
    public void groupByTestWithFilter() throws Exception {
        Aggregations result = query(String.format("SELECT COUNT(*) FROM %s/account GROUP BY filter(gender='m'),gender", TEST_INDEX_ACCOUNT));
        InternalFilter filter = result.get("filter(gender = 'm')@FILTER");
        Terms gender = filter.getAggregations().get("gender");

        for(Terms.Bucket bucket : gender.getBuckets()) {
            String key = bucket.getKey().toString();
            long count = ((ValueCount) bucket.getAggregations().get("COUNT(*)")).getValue();
            if(key.equalsIgnoreCase("m")) {
                Assert.assertEquals(507, count);
            }
            else {
                throw new Exception(String.format("Unexpected key. expected: only m. found: %s", key));
            }
        }
    }


    @Test
    public void groupByOnNestedFieldWithFilterTest() throws Exception {
        Aggregations result = query(String.format("SELECT COUNT(*) FROM %s/nestedType GROUP BY  nested(message.info),filter('myFilter',message.info = 'a')", TEST_INDEX_NESTED_TYPE));
        InternalNested nested = result.get("message.info@NESTED");
        InternalFilter filter = nested.getAggregations().get("myFilter@FILTER");
        Terms infos = filter.getAggregations().get("message.info");
        Assert.assertEquals(1,infos.getBuckets().size());
        for(Terms.Bucket bucket : infos.getBuckets()) {
            String key = bucket.getKey().toString();
            long count = ((ValueCount) bucket.getAggregations().get("COUNT(*)")).getValue();
            if(key.equalsIgnoreCase("a")) {
                Assert.assertEquals(2, count);
            }

            else {
                throw new Exception(String.format("Unexpected key. expected: only a . found: %s", key));
            }
        }
    }

    @Test
    public void minOnNestedField() throws Exception {
        Aggregations result = query(String.format("SELECT min(nested(message.dayOfWeek)) as minDays FROM %s/nestedType", TEST_INDEX_NESTED_TYPE));
        InternalNested nested = result.get("message.dayOfWeek@NESTED");
        Min mins = nested.getAggregations().get("minDays");
        Assert.assertEquals(1.0,mins.getValue(),0.0001);

    }

    @Test
    public void sumOnNestedField() throws Exception {
        Aggregations result = query(String.format("SELECT sum(nested(message.dayOfWeek)) as sumDays FROM %s/nestedType", TEST_INDEX_NESTED_TYPE));
        InternalNested nested = result.get("message.dayOfWeek@NESTED");
        Sum sum = nested.getAggregations().get("sumDays");
        Assert.assertEquals(13.0,sum.getValue(),0.0001);

    }

    @Test
    public void histogramOnNestedField() throws Exception {
        Aggregations result = query(String.format("select count(*) from %s/nestedType group by histogram('field'='message.dayOfWeek','nested'='message','interval'='2' , 'alias' = 'someAlias' )", TEST_INDEX_NESTED_TYPE));
        InternalNested nested  = result.get("message@NESTED");
        Histogram histogram = nested.getAggregations().get("someAlias");
        for(Histogram.Bucket bucket : histogram.getBuckets()){
            long count = ((ValueCount) bucket.getAggregations().get("COUNT(*)")).getValue();
            String key = ((Double)bucket.getKey()).intValue()+"";
            if(key.equals("0") || key.equals("4")){
                Assert.assertEquals(2,count);
            }
            else if (key.equals("2")){
                Assert.assertEquals(1,count);
            }
            else{
                Assert.assertTrue("only 0 2 4 keys are allowed got:" + key,false);
            }
        }


    }

    @Test
    public void reverseToRootGroupByOnNestedFieldWithFilterTestWithReverseNestedAndEmptyPath() throws Exception {
        Aggregations result = query(String.format("SELECT COUNT(*) FROM %s/nestedType GROUP BY  nested(message.info),filter('myFilter',message.info = 'a'),reverse_nested(someField,'')", TEST_INDEX_NESTED_TYPE));
        InternalNested nested = result.get("message.info@NESTED");
        InternalFilter filter = nested.getAggregations().get("myFilter@FILTER");
        Terms infos = filter.getAggregations().get("message.info");
        Assert.assertEquals(1,infos.getBuckets().size());
        for(Terms.Bucket bucket : infos.getBuckets()) {
            InternalReverseNested reverseNested = bucket.getAggregations().get("someField@NESTED");
            Terms terms = reverseNested.getAggregations().get("someField");
            Terms.Bucket internalBucket = terms.getBuckets().get(0);

            long count = ((ValueCount) internalBucket.getAggregations().get("COUNT(*)")).getValue();
            String key = internalBucket.getKey().toString();
            if(key.equalsIgnoreCase("b")) {
                Assert.assertEquals(2, count);
            }
            else {
                throw new Exception(String.format("Unexpected key. expected: only a . found: %s", key));
            }
        }
    }
    @Test
    public void reverseToRootGroupByOnNestedFieldWithFilterTestWithReverseNestedNoPath() throws Exception {
        Aggregations result = query(String.format("SELECT COUNT(*) FROM %s/nestedType GROUP BY  nested(message.info),filter('myFilter',message.info = 'a'),reverse_nested(someField)", TEST_INDEX_NESTED_TYPE));
        InternalNested nested = result.get("message.info@NESTED");
        InternalFilter filter = nested.getAggregations().get("myFilter@FILTER");
        Terms infos = filter.getAggregations().get("message.info");
        Assert.assertEquals(1,infos.getBuckets().size());
        for(Terms.Bucket bucket : infos.getBuckets()) {
            InternalReverseNested reverseNested = bucket.getAggregations().get("someField@NESTED");
            Terms terms = reverseNested.getAggregations().get("someField");
            Terms.Bucket internalBucket = terms.getBuckets().get(0);

            long count = ((ValueCount) internalBucket.getAggregations().get("COUNT(*)")).getValue();
            String key = internalBucket.getKey().toString();
            if(key.equalsIgnoreCase("b")) {
                Assert.assertEquals(2, count);
            }
            else {
                throw new Exception(String.format("Unexpected key. expected: only a . found: %s", key));
            }
        }
    }

    @Test
    public void reverseToRootGroupByOnNestedFieldWithFilterTestWithReverseNestedOnHistogram() throws Exception {
        Aggregations result = query(String.format("SELECT COUNT(*) FROM %s/nestedType GROUP BY  nested(message.info),filter('myFilter',message.info = 'a'),histogram('field'='myNum','reverse_nested'='','interval'='2' , 'alias' = 'someAlias' )", TEST_INDEX_NESTED_TYPE));
        InternalNested nested = result.get("message.info@NESTED");
        InternalFilter filter = nested.getAggregations().get("myFilter@FILTER");
        Terms infos = filter.getAggregations().get("message.info");
        Assert.assertEquals(1,infos.getBuckets().size());
        for(Terms.Bucket bucket : infos.getBuckets()) {
            InternalReverseNested reverseNested = bucket.getAggregations().get("someAlias@NESTED");
            InternalHistogram histogram = reverseNested.getAggregations().get("someAlias");
            Assert.assertEquals(3, histogram.getBuckets().size());

        }
    }

    @Test
    public void reverseToRootGroupByOnNestedFieldWithFilterAndSumOnReverseNestedField() throws Exception {
        Aggregations result = query(String.format("SELECT sum(reverse_nested(myNum)) bla FROM %s/nestedType GROUP BY  nested(message.info),filter('myFilter',message.info = 'a')", TEST_INDEX_NESTED_TYPE));
        InternalNested nested = result.get("message.info@NESTED");
        InternalFilter filter = nested.getAggregations().get("myFilter@FILTER");
        Terms infos = filter.getAggregations().get("message.info");
        Assert.assertEquals(1,infos.getBuckets().size());
        for(Terms.Bucket bucket : infos.getBuckets()) {
            InternalReverseNested reverseNested = bucket.getAggregations().get("myNum@NESTED");
            InternalSum sum = reverseNested.getAggregations().get("bla");
            Assert.assertEquals(5.0,sum.getValue(),0.000001);

        }
    }


    @Test
    public void reverseAnotherNestedGroupByOnNestedFieldWithFilterTestWithReverseNestedNoPath() throws Exception {
        Aggregations result = query(String.format("SELECT COUNT(*) FROM %s/nestedType GROUP BY  nested(message.info),filter('myFilter',message.info = 'a'),reverse_nested(comment.data,'~comment')", TEST_INDEX_NESTED_TYPE));
        InternalNested nested = result.get("message.info@NESTED");
        InternalFilter filter = nested.getAggregations().get("myFilter@FILTER");
        Terms infos = filter.getAggregations().get("message.info");
        Assert.assertEquals(1,infos.getBuckets().size());
        for(Terms.Bucket bucket : infos.getBuckets()) {
            InternalReverseNested reverseNested = bucket.getAggregations().get("comment.data@NESTED_REVERSED");
            InternalNested innerNested = reverseNested.getAggregations().get("comment.data@NESTED");
            Terms terms = innerNested.getAggregations().get("comment.data");
            Terms.Bucket internalBucket = terms.getBuckets().get(0);

            long count = ((ValueCount) internalBucket.getAggregations().get("COUNT(*)")).getValue();
            String key = internalBucket.getKey().toString();
            if(key.equalsIgnoreCase("ab")) {
                Assert.assertEquals(2, count);
            }
            else {
                throw new Exception(String.format("Unexpected key. expected: only a . found: %s", key));
            }
        }
    }

    @Test
    public void reverseAnotherNestedGroupByOnNestedFieldWithFilterTestWithReverseNestedOnHistogram() throws Exception {
        Aggregations result = query(String.format("SELECT COUNT(*) FROM %s/nestedType GROUP BY  nested(message.info),filter('myFilter',message.info = 'a'),histogram('field'='comment.likes','reverse_nested'='~comment','interval'='2' , 'alias' = 'someAlias' )", TEST_INDEX_NESTED_TYPE));
        InternalNested nested = result.get("message.info@NESTED");
        InternalFilter filter = nested.getAggregations().get("myFilter@FILTER");
        Terms infos = filter.getAggregations().get("message.info");
        Assert.assertEquals(1,infos.getBuckets().size());
        for(Terms.Bucket bucket : infos.getBuckets()) {
            InternalReverseNested reverseNested = bucket.getAggregations().get("~comment@NESTED_REVERSED");
            InternalNested innerNested = reverseNested.getAggregations().get("~comment@NESTED");
            InternalHistogram histogram = innerNested.getAggregations().get("someAlias");
            Assert.assertEquals(2, histogram.getBuckets().size());

        }
    }

    @Test
    public void reverseAnotherNestedGroupByOnNestedFieldWithFilterAndSumOnReverseNestedField() throws Exception {
        Aggregations result = query(String.format("SELECT sum(reverse_nested(comment.likes,'~comment')) bla FROM %s/nestedType GROUP BY  nested(message.info),filter('myFilter',message.info = 'a')", TEST_INDEX_NESTED_TYPE));
        InternalNested nested = result.get("message.info@NESTED");
        InternalFilter filter = nested.getAggregations().get("myFilter@FILTER");
        Terms infos = filter.getAggregations().get("message.info");
        Assert.assertEquals(1,infos.getBuckets().size());
        for(Terms.Bucket bucket : infos.getBuckets()) {
            InternalReverseNested reverseNested = bucket.getAggregations().get("comment.likes@NESTED_REVERSED");
            InternalNested innerNested = reverseNested.getAggregations().get("comment.likes@NESTED");
            InternalSum sum = innerNested.getAggregations().get("bla");
            Assert.assertEquals(4.0,sum.getValue(),0.000001);

        }
    }


    @Test
    public void docsReturnedTestWithoutDocsHint() throws Exception {
        String query = String.format("SELECT count(*) from %s/account", TEST_INDEX_ACCOUNT);
        SqlElasticSearchRequestBuilder searchRequestBuilder = getSearchRequestBuilder(query);
        SearchResponse response = (SearchResponse) searchRequestBuilder.get();
        Assert.assertEquals(0,response.getHits().getHits().length);
    }

    @Test
    public void docsReturnedTestWithDocsHint() throws Exception {
        String query = String.format("SELECT /*! DOCS_WITH_AGGREGATION(10) */ count(*) from %s/account",TEST_INDEX_ACCOUNT);
        SqlElasticSearchRequestBuilder searchRequestBuilder = getSearchRequestBuilder(query);
        SearchResponse response = (SearchResponse) searchRequestBuilder.get();
        Assert.assertEquals(10,response.getHits().getHits().length);
    }


    @Test
    public void termsWithScript() throws Exception {
        String query = "select count(*), avg(number) from source group by terms('alias'='asdf', substring(field, 0, 1)), date_histogram('alias'='time', 'field'='timestamp', 'interval'='20d ', 'format'='yyyy-MM-dd') limit 1000";
        String result = MainTestSuite.getSearchDao().explain(query).explain().toString();
        Assert.assertTrue(result.contains("\"script\":{\"source\""));
        Assert.assertTrue(result.contains("substring(0,1)"));
    }

    @Test
    public void groupByScriptedDateHistogram() throws Exception {
        String query = "select count(*), avg(number) from source group by date_histogram('alias'='time', ceil(timestamp), 'interval'='20d ', 'format'='yyyy-MM-dd') limit 1000";
        String result = MainTestSuite.getSearchDao().explain(query).explain().toString();
        Assert.assertTrue(result.contains("Math.ceil(doc['timestamp'].value);"));
        Assert.assertTrue(result.contains("\"script\":{\"source\""));
    }

    @Test
    public void groupByScriptedHistogram() throws Exception {
	    String query = "select count(*) from source group by histogram('alias'='field', pow(field,1))";
	    String result = MainTestSuite.getSearchDao().explain(query).explain().toString();
	    System.out.println(result);
	    Assert.assertTrue(result.contains("Math.pow(doc['field'].value, 1)"));
        Assert.assertTrue(result.contains("\"script\":{\"source\""));
    }

    @Test
    public void groupByTestWithFilters() throws Exception {
        Aggregations result = query(String.format("SELECT COUNT(*) FROM %s/account GROUP BY filters(gender,other,filter(male,gender='m'),filter(female,gender='f'))", TEST_INDEX_ACCOUNT));
        InternalFilters filters = result.get("gender@FILTERS");
        for (InternalFilters.InternalBucket bucket : filters.getBuckets()) {
            String key = bucket.getKey();
            long count = ((ValueCount) bucket.getAggregations().get("COUNT(*)")).getValue();
            if (key.equalsIgnoreCase("male@FILTER")) {
                Assert.assertEquals(507, count);
            } else if (key.equalsIgnoreCase("female@FILTER")) {
                Assert.assertEquals(493, count);
            } else {
                throw new Exception(String.format("Unexpected key: %s", key));
            }
        }
    }
}
