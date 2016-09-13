package org.nlpcn.es4sql.query.maker;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGridBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.NestedBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeBuilder;
import org.elasticsearch.search.aggregations.bucket.range.date.DateRangeBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValuesSourceMetricsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBoundsBuilder;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentilesBuilder;
import org.elasticsearch.search.aggregations.metrics.scripted.ScriptedMetricBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.nlpcn.es4sql.Util;
import org.nlpcn.es4sql.domain.Field;
import org.nlpcn.es4sql.domain.KVValue;
import org.nlpcn.es4sql.domain.MethodField;
import org.nlpcn.es4sql.domain.Where;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.parse.NestedType;
import org.nlpcn.es4sql.query.join.NestedLoopsElasticRequestBuilder;

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

            MethodField methodField = (MethodField) field;
            if(methodField.getName().equals("filter")){
                Map<String, Object> paramsAsMap = methodField.getParamsAsMap();
                Where where = (Where) paramsAsMap.get("where");
                return AggregationBuilders.filter(paramsAsMap.get("alias").toString()).
                        filter(FilterMaker.explan(where));
            }
            return makeRangeGroup(methodField);
		} else {
			TermsBuilder termsBuilder = AggregationBuilders.terms(field.getName()).field(field.getName());
			groupMap.put(field.getName(), new KVValue("KEY", termsBuilder));
			return termsBuilder;
		}
	}


	/**
	 * Create aggregation according to the SQL function.
	 * @param field SQL function
	 * @param parent parentAggregation
	 * @return AggregationBuilder represents the SQL function
	 * @throws SqlParseException in case of unrecognized function
	 */
	public AbstractAggregationBuilder makeFieldAgg(MethodField field, AbstractAggregationBuilder parent) throws SqlParseException {
		groupMap.put(field.getAlias(), new KVValue("FIELD", parent));
        ValuesSourceMetricsAggregationBuilder builder;
        field.setAlias(fixAlias(field.getAlias()));
		switch (field.getName().toUpperCase()) {
		case "SUM":
			builder = AggregationBuilders.sum(field.getAlias());
            return addFieldOrScriptToAggregation(field, builder);
		case "MAX":
			builder = AggregationBuilders.max(field.getAlias());
            return addFieldOrScriptToAggregation(field, builder);
		case "MIN":
			builder =  AggregationBuilders.min(field.getAlias());
            return addFieldOrScriptToAggregation(field, builder);
		case "AVG":
			builder =  AggregationBuilders.avg(field.getAlias());
            return addFieldOrScriptToAggregation(field, builder);
		case "STATS":
			builder =  AggregationBuilders.stats(field.getAlias());
            return addFieldOrScriptToAggregation(field, builder);
        case "EXTENDED_STATS":
            builder =  AggregationBuilders.extendedStats(field.getAlias());
            return addFieldOrScriptToAggregation(field, builder);
        case "PERCENTILES":
            builder = AggregationBuilders.percentiles(field.getAlias());
            addSpecificPercentiles((PercentilesBuilder) builder,field.getParams());
            return addFieldOrScriptToAggregation(field, builder);
		case "TOPHITS":
			return makeTopHitsAgg(field);
        case "SCRIPTED_METRIC":
            return scriptedMetric(field);
		case "COUNT":
			groupMap.put(field.getAlias(), new KVValue("COUNT", parent));
			return makeCountAgg(field);
		default:
			throw new SqlParseException("the agg function not to define !");
		}
	}

    private void addSpecificPercentiles(PercentilesBuilder percentilesBuilder, List<KVValue> params) {
        List<Double> percentiles = new ArrayList<>();
        for(KVValue kValue : params ){
            if(kValue.value.getClass().equals(BigDecimal.class)){
                BigDecimal percentile = (BigDecimal) kValue.value;
                percentiles.add(percentile.doubleValue());

            }
        }
        if(percentiles.size() > 0) {
            double[] percentilesArr = new double[percentiles.size()];
            int i=0;
            for (Double percentile : percentiles){
                percentilesArr[i] = percentile;
                i++;
            }
            percentilesBuilder.percentiles(percentilesArr);
        }
    }

    private String fixAlias(String alias) {
        //because [ is not legal as alias
        return alias.replaceAll("\\[","(").replaceAll("\\]",")");
    }

    private AbstractAggregationBuilder addFieldOrScriptToAggregation(MethodField field, ValuesSourceMetricsAggregationBuilder builder) {
        KVValue kvValue = field.getParams().get(0);
        if(kvValue.key!=null && kvValue.key.equals("script") ){
            //todo: support different lang script
           return builder.script(((MethodField)kvValue.value).getParams().get(1).toString());
        }
        else  if (kvValue.key!=null && ( kvValue.key.equals("nested") || kvValue.key.equals("reverse_nested")) ){
            NestedType nestedType = (NestedType) kvValue.value;
            builder.field(nestedType.field);
            AggregationBuilder nestedBuilder;
            String nestedAggName = nestedType.field + "@NESTED";
            if(nestedType.isReverse()) {
                if(nestedType.path!=null && nestedType.path.startsWith("~")){
                    String realPath = nestedType.path.substring(1);
                    nestedBuilder = AggregationBuilders.nested(nestedAggName).path(realPath);
                    nestedBuilder = nestedBuilder.subAggregation(builder);
                    return AggregationBuilders.reverseNested(nestedAggName+"_REVERSED").subAggregation(nestedBuilder);
                }
                else{
                    nestedBuilder = AggregationBuilders.reverseNested(nestedAggName).path(nestedType.path);
                }
            }
            else {
                nestedBuilder = AggregationBuilders.nested(nestedAggName).path(nestedType.path);
            }
            return nestedBuilder.subAggregation(builder);
        }
        return builder.field(kvValue.toString());
    }

    private AggregationBuilder<?> makeRangeGroup(MethodField field) throws SqlParseException {

        AggregationBuilder<?> aggregationBuilder;
		switch (field.getName().toLowerCase()) {
		case "range":
            aggregationBuilder = rangeBuilder(field);break;
		case "date_histogram":
            aggregationBuilder = dateHistogram(field);break;
		case "date_range":
            aggregationBuilder = dateRange(field);break;
		case "month":
            aggregationBuilder = dateRange(field);break;
		case "histogram":
            aggregationBuilder = histogram(field);break;
        case "geohash_grid":
            aggregationBuilder = geohashGrid(field);break;
        case "geo_bounds":
                aggregationBuilder = geoBounds(field);break;
        case "terms":
                aggregationBuilder = termsAgg(field);break;
		default:
			throw new SqlParseException("can define this method " + field);
		}
       return aggregationBuilder;

	}

    private AggregationBuilder<?> geoBounds(MethodField field) throws SqlParseException {
        String aggName = gettAggNameFromParamsOrAlias(field);
        GeoBoundsBuilder boundsBuilder = AggregationBuilders.geoBounds(aggName);
        String value = null;
        for (KVValue kv : field.getParams()) {
            value = kv.value.toString();
            switch (kv.key.toLowerCase()) {
                case "field":
                    boundsBuilder.field(value);
                    break;
                case "wrap_longitude":
                    boundsBuilder.wrapLongitude(Boolean.getBoolean(value));
                    break;
                case "alias":
                case "nested":
                case "reverse_nested":
                    break;
                default:
                    throw new SqlParseException("geo_bounds err or not define field " + kv.toString());
            }
        }
        return boundsBuilder;
    }

    private AggregationBuilder<?> termsAgg(MethodField field) throws SqlParseException {
        String aggName = gettAggNameFromParamsOrAlias(field);
        TermsBuilder terms = AggregationBuilders.terms(aggName);
        String value = null;
        for (KVValue kv : field.getParams()) {
            value = kv.value.toString();
            switch (kv.key.toLowerCase()) {
                case "field":
                    terms.field(value);
                    break;
                case "size":
                    terms.size(Integer.parseInt(value));
                    break;
                case "shard_size":
                    terms.shardSize(Integer.parseInt(value));
                    break;
                case "min_doc_count":
                    terms.minDocCount(Integer.parseInt(value));
                    break;
                case "alias":
                case "nested":
                case "reverse_nested":
                    break;
                default:
                    throw new SqlParseException("terms aggregation err or not define field " + kv.toString());
            }
        }
        return terms;
    }

    private AbstractAggregationBuilder scriptedMetric(MethodField field) throws SqlParseException {
        String aggName = gettAggNameFromParamsOrAlias(field);
        ScriptedMetricBuilder scriptedMetricBuilder = AggregationBuilders.scriptedMetric(aggName);
        Map<String, Object> scriptedMetricParams = field.getParamsAsMap();
        if(!scriptedMetricParams.containsKey("map_script") && !scriptedMetricParams.containsKey("map_script_id") && !scriptedMetricParams.containsKey("map_script_file")){
            throw new SqlParseException("scripted metric parameters must contain map_script/map_script_id/map_script_file parameter");
        }
        HashMap<String,Object> scriptAdditionalParams = new HashMap<>();
        HashMap<String,Object> reduceScriptAdditionalParams = new HashMap<>();
        for(Map.Entry<String,Object> param : scriptedMetricParams.entrySet()) {
            String paramValue = param.getValue().toString();
            if(param.getKey().startsWith("@")){
                if(param.getKey().startsWith("@reduce_")){
                    reduceScriptAdditionalParams.put(param.getKey().replace("@reduce_",""),param.getValue());
                }
                else{
                    scriptAdditionalParams.put(param.getKey().replace("@",""),param.getValue());
                }
               continue;
            }

            switch (param.getKey().toLowerCase()) {
                case "map_script":
                    scriptedMetricBuilder.mapScript(paramValue);
                    break;
                case "map_script_id":
                    scriptedMetricBuilder.mapScriptId(paramValue);
                    break;
                case "map_script_file":
                    scriptedMetricBuilder.mapScriptFile(paramValue);
                    break;
                case "init_script":
                    scriptedMetricBuilder.initScript(paramValue);
                    break;
                case "init_script_id":
                    scriptedMetricBuilder.initScriptId(paramValue);
                    break;
                case "init_script_file":
                    scriptedMetricBuilder.initScriptFile(paramValue);
                    break;
                case "combine_script":
                    scriptedMetricBuilder.combineScript(paramValue);
                    break;
                case "combine_script_id":
                    scriptedMetricBuilder.combineScriptId(paramValue);
                    break;
                case "combine_script_file":
                    scriptedMetricBuilder.combineScriptFile(paramValue);
                    break;
                case "reduce_script":
                    scriptedMetricBuilder.reduceScript(paramValue);
                    break;
                case "reduce_script_id":
                    scriptedMetricBuilder.reduceScriptId(paramValue);
                    break;
                case "reduce_script_file":
                    scriptedMetricBuilder.reduceScriptFile(paramValue);
                    break;
                case "alias":
                case "nested":
                case "reverse_nested":
                    break;
                default:
                    throw new SqlParseException("scripted_metric err or not define field " + param.getKey());
            }
        }
        if(scriptAdditionalParams.size() > 0){
            scriptAdditionalParams.put("_agg",new HashMap<>());
            scriptedMetricBuilder.params(scriptAdditionalParams);
        }

        if(reduceScriptAdditionalParams.size() > 0)
            scriptedMetricBuilder.reduceParams(reduceScriptAdditionalParams);
        return scriptedMetricBuilder;
    }

    private AggregationBuilder<?> geohashGrid(MethodField field) throws SqlParseException {
        String aggName = gettAggNameFromParamsOrAlias(field);
        GeoHashGridBuilder geoHashGrid = AggregationBuilders.geohashGrid(aggName);
        String value = null;
        for (KVValue kv : field.getParams()) {
            value = kv.value.toString();
            switch (kv.key.toLowerCase()) {
                case "precision":
                    geoHashGrid.precision(Integer.parseInt(value));
                    break;
                case "field":
                    geoHashGrid.field(value);
                    break;
                case "size":
                    geoHashGrid.size(Integer.parseInt(value));
                    break;
                case "shard_size":
                    geoHashGrid.shardSize(Integer.parseInt(value));
                    break;
                case "alias":
                case "nested":
                case "reverse_nested":
                    break;
                default:
                    throw new SqlParseException("geohash grid err or not define field " + kv.toString());
            }
        }
        return geoHashGrid;
    }

    private static final String TIME_FARMAT = "yyyy-MM-dd HH:mm:ss";

	private ValuesSourceAggregationBuilder<?> dateRange(MethodField field) {
        String alias = gettAggNameFromParamsOrAlias(field);
		DateRangeBuilder dateRange = AggregationBuilders.dateRange(alias).format(TIME_FARMAT);

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
            } else if ("alias".equals(kv.key)){
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
        String alias = gettAggNameFromParamsOrAlias(field);
		DateHistogramBuilder dateHistogram = AggregationBuilders.dateHistogram(alias).format(TIME_FARMAT);
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
                case "alias":
                case "nested":
                case "reverse_nested":
                    break;
			default:
				throw new SqlParseException("date range err or not define field " + kv.toString());
			}
		}
		return dateHistogram;
	}

    private String gettAggNameFromParamsOrAlias(MethodField field) {
        String alias = field.getAlias();
        for (KVValue kv : field.getParams()) {
            if(kv.key != null &&kv.key.equals("alias"))
                alias = kv.value.toString();
        }
        return alias;
    }

    private HistogramBuilder histogram(MethodField field) throws SqlParseException {
        String aggName = gettAggNameFromParamsOrAlias(field);
		HistogramBuilder histogram = AggregationBuilders.histogram(aggName);
		String value = null;
		for (KVValue kv : field.getParams()) {
			value = kv.value.toString();
			switch (kv.key.toLowerCase()) {
				case "interval":
					histogram.interval(Long.parseLong(value));
					break;
				case "field":
					histogram.field(value);
					break;
				case "min_doc_count":
					histogram.minDocCount(Long.parseLong(value));
					break;
				case "extended_bounds":
					String[] bounds = value.split(":");
					if (bounds.length == 2)
						histogram.extendedBounds(Long.valueOf(bounds[0]), Long.valueOf(bounds[1]));
					break;
                case "alias":
                case "nested":
                case "reverse_nested":
                    break;
				case "order":
					Histogram.Order order = null;
					switch (value) {
						case "key_desc":
							order = Histogram.Order.KEY_DESC;
							break;
						case "count_asc":
							order = Histogram.Order.COUNT_ASC;
							break;
						case "count_desc":
							order = Histogram.Order.COUNT_DESC;
							break;
						case "key_asc":
						default:
							order = Histogram.Order.KEY_ASC;
							break;
					}
					histogram.order(order);
					break;
				default:
					throw new SqlParseException("histogram err or not define field " + kv.toString());
			}
		}
		return histogram;
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
	 * Create count aggregation.
	 * @param field The count function
	 * @return AggregationBuilder use to count result
	 */
	private AbstractAggregationBuilder makeCountAgg(MethodField field) {

		// Cardinality is approximate DISTINCT.
		if ("DISTINCT".equals(field.getOption())) {
			return AggregationBuilders.cardinality(field.getAlias()).precisionThreshold(40000).field(field.getParams().get(0).value.toString());
		}

		String fieldName = field.getParams().get(0).value.toString();

		// In case of count(*) we use '_index' as field parameter to count all documents
		if ("*".equals(fieldName)) {
			return AggregationBuilders.count(field.getAlias()).field("_index");
		}
		else {
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
        String alias = gettAggNameFromParamsOrAlias(field);
		TopHitsBuilder topHits = AggregationBuilders.topHits(alias);
		List<KVValue> params = field.getParams();
		for (KVValue kv : params) {
			switch (kv.key) {
			case "from":
				topHits.setFrom((int) kv.value);
				break;
			case "size":
				topHits.setSize((int) kv.value);
				break;
            case "alias":
            case "nested":
            case "reverse_nested":
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
