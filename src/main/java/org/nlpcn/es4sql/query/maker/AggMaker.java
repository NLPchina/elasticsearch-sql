package org.nlpcn.es4sql.query.maker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeBuilder;
import org.elasticsearch.search.aggregations.bucket.range.date.DateRangeBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.nlpcn.es4sql.Util;
import org.nlpcn.es4sql.domain.Field;
import org.nlpcn.es4sql.domain.KVValue;
import org.nlpcn.es4sql.domain.MethodField;
import org.nlpcn.es4sql.exception.SqlParseException;

public class AggMaker {

	private Map<String, KVValue> groupMap = new HashMap<>();

	/**
	 * 分组查的聚合函数
	 * 
	 * @param field
	 * @return
	 * @throws SqlParseException
	 */
	public AggregationBuilder<?> makeGroupAgg(Field field) throws SqlParseException {
		if (field instanceof MethodField) {
			return makeRangeGroup((MethodField) field);
		} else {
			TermsBuilder termsBuilder = AggregationBuilders.terms(field.getName()).field(field.getName());
			groupMap.put(field.getName(), new KVValue("KEY", termsBuilder));
			return termsBuilder;
		}
	}

	/**
	 * 将Field 转换为agg
	 * 
	 * @param field
	 * @return
	 * @throws SqlParseException
	 */
	public AbstractAggregationBuilder makeFieldAgg(MethodField field, AbstractAggregationBuilder parent) throws SqlParseException {
		groupMap.put(field.getAlias(), new KVValue("FIELD", parent));
		switch (field.getName().toUpperCase()) {
		case "SUM":
			return AggregationBuilders.sum(field.getAlias()).field(field.getParams().get(0).toString());
		case "MAX":
			return AggregationBuilders.max(field.getAlias()).field(field.getParams().get(0).toString());
		case "MIN":
			return AggregationBuilders.min(field.getAlias()).field(field.getParams().get(0).toString());
		case "AVG":
			return AggregationBuilders.avg(field.getAlias()).field(field.getParams().get(0).toString());
		case "TOPHITS":
			return makeTopHitsAgg(field);
		case "COUNT":
			// 如果是count修正排序方式
			groupMap.put(field.getAlias(), new KVValue("COUNT", parent));
			return makeCountAgg(field);
		default:
			throw new SqlParseException("the agg function not to define !");
		}
	}

	private ValuesSourceAggregationBuilder<?> makeRangeGroup(MethodField field) throws SqlParseException {
		switch (field.getName().toLowerCase()) {
		case "range":
			return rangeBuilder(field);
		case "date_histogram":
			return dateHistogram(field);
		case "date_range":
			return dateRange(field);
		default:
			throw new SqlParseException("can define this method " + field);
		}

	}

	private static final String TIME_FARMAT = "yyyy-MM-dd HH:mm:ss";

	private ValuesSourceAggregationBuilder<?> dateRange(MethodField field) {
		DateRangeBuilder dateRange = AggregationBuilders.dateRange(field.getAlias()).format(TIME_FARMAT);

		String value = null;
		List<String> ranges = new ArrayList<>();
		for (KVValue kv : field.getParams()) {
			value = kv.value.toString();
			if ("field".equals(kv.key)) {
				dateRange.field(value);
				continue;
			} else if ("format".equals(kv.key)) {
				dateRange.format(value);
				continue;
			} else if ("from".equals(kv.key)) {
				dateRange.addUnboundedFrom(kv.value);
				continue;
			} else if ("to".equals(kv.key)) {
				dateRange.addUnboundedTo(kv.value);
				continue;
			} else {
				ranges.add(value);
			}
		}

		for (int i = 1; i < ranges.size(); i++) {
			dateRange.addRange(ranges.get(i - 1), ranges.get(i));
		}

		return dateRange;
	}

	/**
	 * 按照时间范围分组
	 * 
	 * @param field
	 * @return
	 * @throws SqlParseException
	 */
	private DateHistogramBuilder dateHistogram(MethodField field) throws SqlParseException {
		DateHistogramBuilder dateHistogram = AggregationBuilders.dateHistogram(field.getAlias()).format(TIME_FARMAT);
		String value = null;
		for (KVValue kv : field.getParams()) {
			value = kv.value.toString();
			switch (kv.key.toLowerCase()) {
			case "interval":
				dateHistogram.interval(new DateHistogram.Interval(kv.value.toString()));
				break;
			case "field":
				dateHistogram.field(value);
				break;
			case "format":
				dateHistogram.format(value);
				break;
			case "time_zone":
			case "pre_zone":
				dateHistogram.preZone(value);
				break;
			case "post_zone":
				dateHistogram.postZone(value);
				break;
			case "post_offset":
				dateHistogram.postOffset(value);
				break;
			case "pre_offset":
				dateHistogram.preOffset(value);
				break;
			default:
				throw new SqlParseException("date range err or not define field " + kv.toString());
			}
		}
		return dateHistogram;
	}

	/**
	 * 构建范围查询
	 * 
	 * @param field
	 * @return
	 */
	private RangeBuilder rangeBuilder(MethodField field) {

		LinkedList<KVValue> params = new LinkedList<>(field.getParams());

		String fieldName = params.poll().toString();

		double[] ds = Util.KV2DoubleArr(params);

		RangeBuilder range = AggregationBuilders.range(field.getAlias()).field(fieldName);

		for (int i = 1; i < ds.length; i++) {
			range.addRange(ds[i - 1], ds[i]);
		}

		return range;
	}

	/**
	 * 构建count查询
	 * 
	 * @param field
	 * @return
	 */
	private AbstractAggregationBuilder makeCountAgg(MethodField field) {
		if ("DISTINCT".equals(field.getOption())) {
			return AggregationBuilders.cardinality(field.getAlias()).field(field.getParams().get(0).value.toString());
		}
		String fieldName = field.getParams().get(0).value.toString();
		if ("*".equals(fieldName)) {
			return AggregationBuilders.count(field.getAlias());
		} else {
			return AggregationBuilders.count(field.getAlias()).field(fieldName);
		}
	}

	/**
	 * TOPHITS查询
	 * 
	 * @param field
	 * @return
	 */
	private AbstractAggregationBuilder makeTopHitsAgg(MethodField field) {
		TopHitsBuilder topHits = AggregationBuilders.topHits(field.getAlias());
		List<KVValue> params = field.getParams();
		for (KVValue kv : params) {
			switch (kv.key) {
			case "from":
				topHits.setFrom((int) kv.value);
				break;
			case "size":
				topHits.setSize((int) kv.value);
				break;
			default:
				topHits.addSort(kv.key, SortOrder.valueOf(kv.value.toString().toUpperCase()));
				break;
			}
		}
		return topHits;
	}

	public Map<String, KVValue> getGroupMap() {
		return this.groupMap;
	}

}
