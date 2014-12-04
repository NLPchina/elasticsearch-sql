package org.nlpcn.es4sql;

import java.io.IOException;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;

public class AggregationTest {

	private SearchDao searchDao = new SearchDao();

	@Test
	public void sumDistinctOrderTest() throws IOException, SqlParseException {
		SearchRequestBuilder select = searchDao.explan("select sum(age),count(*), count(distinct age) from bank  group by gender order by count(distinct age)  desc  limit 3");
		System.out.println(select);
	}

	@Test
	public void sumSortAliasCount() throws IOException, SqlParseException {
		SearchRequestBuilder select = searchDao.explan("select sum(age),count(*) as kk, count(age) as k from bank  group by gender order by kk asc limit 10 ");
		System.out.println(select);
	}

	@Test
	public void sumSortCount() throws IOException, SqlParseException {
		SearchRequestBuilder select = searchDao.explan("select sum(age), count(age)  from bank  group by gender order by count(age) asc limit 2 ");
		System.out.println(select);
	}

	@Test
	public void minTest() throws IOException, SqlParseException {
		SearchRequestBuilder result = searchDao.explan("select min(age) from bank  group by gender ");
		System.out.println(result);
	}

	@Test
	public void maxTest() throws IOException, SqlParseException {
		SearchRequestBuilder result = searchDao.explan("select max(age) from bank  group by gender ");
		System.out.println(result);
	}

	@Test
	public void avgTest() throws IOException, SqlParseException {
		SearchRequestBuilder result = searchDao.explan("select avg(age) from bank  group by gender ");
		System.out.println(result);
	}

	@Test
	public void countGroupByTest() throws IOException, SqlParseException {
		SearchRequestBuilder result = searchDao.explan("select count(*) from bank  group by gender ");
		System.out.println(result);
	}

	/**
	 * 区段group 聚合
	 * 
	 * http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-aggregations-bucket-range-aggregation.html
	 * 
	 * @throws IOException
	 * @throws SqlParseException
	 */
	@Test
	public void countGroupByRange() throws IOException, SqlParseException {
		SearchRequestBuilder result = searchDao.explan("select count(age) from bank  group by range(age, 20,25,30,35,40) ");
		System.out.println(result);
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
	public void countGroupByDateTest() throws IOException, SqlParseException {
		SearchRequestBuilder result = searchDao.explan("select insert_time from online  group by date_histogram(field='insert_time','interval'='1.5h','format'='yyyy-MM') ");
		System.out.println(result);
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
	public void countDateRangeTest() throws IOException, SqlParseException {
		SearchRequestBuilder result = searchDao
				.explan("select online from online  group by date_range(field='insert_time','format'='yyyy-MM-dd' ,'2014-08-18','2014-08-17','now-8d','now-7d','now-6d','now') ");
		System.out.println(result);
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
	public void countTest() throws IOException, SqlParseException {
		SearchRequestBuilder result = searchDao
				.explan("select count(*),sum(all_tv_clinet) from online group by date_range(field='insert_time','format'='yyyy-MM-dd' ,'2014-08-18','2014-08-17','now-8d','now-7d','now-6d','now') ");
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
	public void topHitTest() throws IOException, SqlParseException {
		SearchRequestBuilder result = searchDao.explan("select topHits('size'=3,age='desc') from bank  group by gender ");
		System.out.println(result);
	}

}
