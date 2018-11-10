package org.nlpcn.es4sql.query.maker;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.ZoneOffset;
import java.util.*;

import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.join.aggregations.JoinAggregationBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;

import org.elasticsearch.search.aggregations.bucket.histogram.*;
import org.elasticsearch.search.aggregations.bucket.nested.ReverseNestedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBoundsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentilesAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.scripted.ScriptedMetricAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.joda.time.DateTimeZone;
import org.nlpcn.es4sql.Util;
import org.nlpcn.es4sql.domain.Field;
import org.nlpcn.es4sql.domain.KVValue;
import org.nlpcn.es4sql.domain.MethodField;
import org.nlpcn.es4sql.domain.Where;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.parse.ChildrenType;
import org.nlpcn.es4sql.parse.NestedType;

public class AggMaker {

    //question 这个groupMap用来干嘛？？
    private Map<String, KVValue> groupMap = new HashMap<>();

    /**
     * 分组查的聚合函数
     *
     * @param field
     * @return
     * @throws SqlParseException
     */
    public AggregationBuilder makeGroupAgg(Field field) throws SqlParseException {

        //zhongshu-comment script类型的MethodField
        if (field instanceof MethodField && field.getName().equals("script")) {
            MethodField methodField = (MethodField) field;
                /*
                TermsAggregationBuilder termsBuilder的样例：
                来自这条sql的group by子句的gg字段解析结果：
                select a,case when c='1' then 'haha' when c='2' then 'book' else 'hbhb' end as gg from tbl_a group by a,gg
                {
                    "gg":{ //aggs的名字就叫gg
                        "terms":{
                            "script":{
                                "source":"if((doc['c'].value=='1')){'haha'} else if((doc['c'].value=='2')){'book'} else {'hbhb'}",
                                "lang":"painless"
                            },
                            "size":10,
                            "min_doc_count":1,
                            "shard_min_doc_count":0,
                            "show_term_doc_count_error":false,
                            "order":[
                                {
                                    "_count":"desc"
                                },
                                {
                                    "_key":"asc"
                                }
                            ]
                        }
                    }
                }
             */
            TermsAggregationBuilder termsBuilder = AggregationBuilders.terms(methodField.getAlias()).script(new Script(methodField.getParams().get(1).value.toString()));

            //question 这里为什么要将这些信息加到groupMap中？
            groupMap.put(methodField.getAlias(), new KVValue("KEY", termsBuilder));

            return termsBuilder;
        }

        //zhongshu-comment filter类型的MethodField
        if (field instanceof MethodField) {

            MethodField methodField = (MethodField) field;
            if (methodField.getName().equals("filter")) {
                Map<String, Object> paramsAsMap = methodField.getParamsAsMap();
                Where where = (Where) paramsAsMap.get("where");
                return AggregationBuilders.filter(paramsAsMap.get("alias").toString(),
                        QueryMaker.explan(where));
            }
            return makeRangeGroup(methodField);
        } else {
            TermsAggregationBuilder termsBuilder = AggregationBuilders.terms(field.getName()).field(field.getName());
            groupMap.put(field.getName(), new KVValue("KEY", termsBuilder));
            return termsBuilder;
        }
    }


    /**
     * Create aggregation according to the SQL function.
     * zhongshu-comment 根据sql中的函数来生成一些agg，例如sql中的count()、sum()函数，这是agg链中最里边的那个agg了，eg：
     *                  select a,b,count(c),sum(d) from tbl group by a,b
     * @param field  SQL function
     * @param parent parentAggregation
     * @return AggregationBuilder represents the SQL function
     * @throws SqlParseException in case of unrecognized function
     */
    public AggregationBuilder makeFieldAgg(MethodField field, AggregationBuilder parent) throws SqlParseException {
        //question 加到groupMap里是为了什么
        groupMap.put(field.getAlias(), new KVValue("FIELD", parent));

        ValuesSourceAggregationBuilder builder;
        field.setAlias(fixAlias(field.getAlias()));
        switch (field.getName().toUpperCase()) {
            case "SUM":
                builder = AggregationBuilders.sum(field.getAlias());
                return addFieldToAgg(field, builder);
            case "MAX":
                builder = AggregationBuilders.max(field.getAlias());
                return addFieldToAgg(field, builder);
            case "MIN":
                builder = AggregationBuilders.min(field.getAlias());
                return addFieldToAgg(field, builder);
            case "AVG":
                builder = AggregationBuilders.avg(field.getAlias());
                return addFieldToAgg(field, builder);
            case "STATS":
                builder = AggregationBuilders.stats(field.getAlias());
                return addFieldToAgg(field, builder);
            case "EXTENDED_STATS":
                builder = AggregationBuilders.extendedStats(field.getAlias());
                return addFieldToAgg(field, builder);
            case "PERCENTILES":
                builder = AggregationBuilders.percentiles(field.getAlias());
                addSpecificPercentiles((PercentilesAggregationBuilder) builder, field.getParams());
                return addFieldToAgg(field, builder);
            case "TOPHITS":
                return makeTopHitsAgg(field);
            case "SCRIPTED_METRIC":
                return scriptedMetric(field);
            case "COUNT":
                groupMap.put(field.getAlias(), new KVValue("COUNT", parent));
                return addFieldToAgg(field, makeCountAgg(field));
            default:
                throw new SqlParseException("the agg function not to define !");
        }
    }

    private void addSpecificPercentiles(PercentilesAggregationBuilder percentilesBuilder, List<KVValue> params) {
        List<Double> percentiles = new ArrayList<>();
        for (KVValue kValue : params) {
            if (kValue.value.getClass().equals(BigDecimal.class)) {
                BigDecimal percentile = (BigDecimal) kValue.value;
                percentiles.add(percentile.doubleValue());

            }
        }
        if (percentiles.size() > 0) {
            double[] percentilesArr = new double[percentiles.size()];
            int i = 0;
            for (Double percentile : percentiles) {
                percentilesArr[i] = percentile;
                i++;
            }
            percentilesBuilder.percentiles(percentilesArr);
        }
    }

    private String fixAlias(String alias) {
        //because [ is not legal as alias
        return alias.replaceAll("\\[", "(").replaceAll("\\]", ")");
    }

    private AggregationBuilder addFieldToAgg(MethodField field, ValuesSourceAggregationBuilder builder) {
        KVValue kvValue = field.getParams().get(0);
        if (kvValue.key != null && kvValue.key.equals("script")) {
            if (kvValue.value instanceof MethodField) {
                return builder.script(new Script(((MethodField) kvValue.value).getParams().get(1).toString()));
            } else {
                return builder.script(new Script(kvValue.value.toString()));
            }

        } else if (kvValue.key != null && kvValue.value.toString().trim().startsWith("def")) {
            return builder.script(new Script(kvValue.value.toString()));
        } else if (kvValue.key != null && (kvValue.key.equals("nested") || kvValue.key.equals("reverse_nested"))) {
            NestedType nestedType = (NestedType) kvValue.value;

            builder.field(nestedType.field);

            AggregationBuilder nestedBuilder;

            String nestedAggName = nestedType.field + "@NESTED";

            if (nestedType.isReverse()) {
                if (nestedType.path != null && nestedType.path.startsWith("~")) {
                    String realPath = nestedType.path.substring(1);
                    nestedBuilder = AggregationBuilders.nested(nestedAggName,realPath);
                    nestedBuilder = nestedBuilder.subAggregation(builder);
                    return AggregationBuilders.reverseNested(nestedAggName + "_REVERSED").subAggregation(nestedBuilder);
                } else {
                    ReverseNestedAggregationBuilder reverseNestedAggregationBuilder = AggregationBuilders.reverseNested(nestedAggName);
                    if (nestedType.path!=null){
                        reverseNestedAggregationBuilder.path(nestedType.path);
                    }
                    nestedBuilder = reverseNestedAggregationBuilder;
                }
            } else {
                nestedBuilder = AggregationBuilders.nested(nestedAggName,nestedType.path);
            }

            return nestedBuilder.subAggregation(builder);
        } else if (kvValue.key != null && (kvValue.key.equals("children"))) {
            ChildrenType childrenType = (ChildrenType) kvValue.value;

            builder.field(childrenType.field);

            AggregationBuilder childrenBuilder;

            String childrenAggName = childrenType.field + "@CHILDREN";

            childrenBuilder = JoinAggregationBuilders.children(childrenAggName,childrenType.childType);

            return childrenBuilder;
        }

        return builder.field(kvValue.toString());
    }

    private AggregationBuilder makeRangeGroup(MethodField field) throws SqlParseException {
        switch (field.getName().toLowerCase()) {
            case "range":
                return rangeBuilder(field);
            case "date_histogram":
                return dateHistogram(field);
            case "date_range":
                return dateRange(field);
            case "month":
                return dateRange(field);
            case "histogram":
                return histogram(field);
            case "geohash_grid":
                return geohashGrid(field);
            case "geo_bounds":
                return geoBounds(field);
            case "terms":
                return termsAgg(field);
            default:
                throw new SqlParseException("can define this method " + field);
        }

    }

    private AggregationBuilder geoBounds(MethodField field) throws SqlParseException {
        String aggName = gettAggNameFromParamsOrAlias(field);
        GeoBoundsAggregationBuilder boundsBuilder = AggregationBuilders.geoBounds(aggName);
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
                case "children":
                    break;
                default:
                    throw new SqlParseException("geo_bounds err or not define field " + kv.toString());
            }
        }
        return boundsBuilder;
    }

    private AggregationBuilder termsAgg(MethodField field) throws SqlParseException {
        String aggName = gettAggNameFromParamsOrAlias(field);
        TermsAggregationBuilder terms = AggregationBuilders.terms(aggName);
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
                case "missing":
                    terms.missing(value);
                    break;
                case "order":
                    if ("asc".equalsIgnoreCase(value)) {
                        terms.order(BucketOrder.key(true));
                    } else if ("desc".equalsIgnoreCase(value)) {
                        terms.order(BucketOrder.key(false));
                    } else {
                        throw new SqlParseException("order can only support asc/desc " + kv.toString());
                    }
                    break;
                case "alias":
                case "nested":
                case "reverse_nested":
                case "children":
                    break;
                default:
                    throw new SqlParseException("terms aggregation err or not define field " + kv.toString());
            }
        }
        return terms;
    }

    private AbstractAggregationBuilder scriptedMetric(MethodField field) throws SqlParseException {
        String aggName = gettAggNameFromParamsOrAlias(field);
        ScriptedMetricAggregationBuilder scriptedMetricBuilder = AggregationBuilders.scriptedMetric(aggName);
        Map<String, Object> scriptedMetricParams = field.getParamsAsMap();
        if (!scriptedMetricParams.containsKey("map_script") && !scriptedMetricParams.containsKey("map_script_id") && !scriptedMetricParams.containsKey("map_script_file")) {
            throw new SqlParseException("scripted metric parameters must contain map_script/map_script_id/map_script_file parameter");
        }
        HashMap<String, Object> scriptAdditionalParams = new HashMap<>();
        HashMap<String, Object> reduceScriptAdditionalParams = new HashMap<>();
        for (Map.Entry<String, Object> param : scriptedMetricParams.entrySet()) {
            String paramValue = param.getValue().toString();
            if (param.getKey().startsWith("@")) {
                if (param.getKey().startsWith("@reduce_")) {
                    reduceScriptAdditionalParams.put(param.getKey().replace("@reduce_", ""), param.getValue());
                } else {
                    scriptAdditionalParams.put(param.getKey().replace("@", ""), param.getValue());
                }
                continue;
            }

            switch (param.getKey().toLowerCase()) {
                case "map_script":
                    scriptedMetricBuilder.mapScript(new Script(paramValue));
                    break;
                case "map_script_id":
                    scriptedMetricBuilder.mapScript(new Script(ScriptType.STORED, Script.DEFAULT_SCRIPT_LANG,paramValue, new HashMap<String, Object>()));
                    break;
                case "init_script":
                    scriptedMetricBuilder.initScript(new Script(paramValue));
                    break;
                case "init_script_id":
                    scriptedMetricBuilder.initScript(new Script(ScriptType.STORED,Script.DEFAULT_SCRIPT_LANG,paramValue, new HashMap<String, Object>()));
                    break;
                case "combine_script":
                    scriptedMetricBuilder.combineScript(new Script(paramValue));
                    break;
                case "combine_script_id":
                    scriptedMetricBuilder.combineScript(new Script(ScriptType.STORED, Script.DEFAULT_SCRIPT_LANG,paramValue, new HashMap<String, Object>()));
                    break;
                case "reduce_script":
                    scriptedMetricBuilder.reduceScript(new Script(ScriptType.INLINE,  Script.DEFAULT_SCRIPT_LANG , paramValue, reduceScriptAdditionalParams));
                    break;
                case "reduce_script_id":
                    scriptedMetricBuilder.reduceScript(new Script(ScriptType.STORED,  Script.DEFAULT_SCRIPT_LANG,paramValue, reduceScriptAdditionalParams));
                    break;
                case "alias":
                case "nested":
                case "reverse_nested":
                case "children":
                    break;
                default:
                    throw new SqlParseException("scripted_metric err or not define field " + param.getKey());
            }
        }
        if (scriptAdditionalParams.size() > 0) {
            scriptAdditionalParams.put("_agg", new HashMap<>());
            scriptedMetricBuilder.params(scriptAdditionalParams);
        }

        return scriptedMetricBuilder;
    }

    private AggregationBuilder geohashGrid(MethodField field) throws SqlParseException {
        String aggName = gettAggNameFromParamsOrAlias(field);
        GeoGridAggregationBuilder geoHashGrid = AggregationBuilders.geohashGrid(aggName);
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
                case "children":
                    break;
                default:
                    throw new SqlParseException("geohash grid err or not define field " + kv.toString());
            }
        }
        return geoHashGrid;
    }

    private static final String TIME_FARMAT = "yyyy-MM-dd HH:mm:ss";

    private ValuesSourceAggregationBuilder dateRange(MethodField field) {
        String alias = gettAggNameFromParamsOrAlias(field);
        DateRangeAggregationBuilder dateRange = AggregationBuilders.dateRange(alias).format(TIME_FARMAT);

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
            } else if ("time_zone".equals(kv.key)) {
                dateRange.timeZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone(ZoneOffset.of(value))));
                continue;
            } else if ("from".equals(kv.key)) {
                dateRange.addUnboundedFrom(kv.value.toString());
                continue;
            } else if ("to".equals(kv.key)) {
                dateRange.addUnboundedTo(kv.value.toString());
                continue;
            } else if ("alias".equals(kv.key) || "nested".equals(kv.key) || "children".equals(kv.key)) {
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
    private DateHistogramAggregationBuilder dateHistogram(MethodField field) throws SqlParseException {
        String alias = gettAggNameFromParamsOrAlias(field);
        DateHistogramAggregationBuilder dateHistogram = AggregationBuilders.dateHistogram(alias).format(TIME_FARMAT);
        String value = null;
        for (KVValue kv : field.getParams()) {
            value = kv.value.toString();
            switch (kv.key.toLowerCase()) {
                case "interval":
                    dateHistogram.dateHistogramInterval(new DateHistogramInterval(kv.value.toString()));
                    break;
                case "field":
                    dateHistogram.field(value);
                    break;
                case "format":
                    dateHistogram.format(value);
                    break;
                case "time_zone":
                    dateHistogram.timeZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone(ZoneOffset.of(value))));
                    break;
                case "min_doc_count":
                    dateHistogram.minDocCount(Long.parseLong(value));
                    break;
                case "order":
                    dateHistogram.order("desc".equalsIgnoreCase(value) ? BucketOrder.key(false) : BucketOrder.key(true));
                    break;
                case "extended_bounds":
                    String[] bounds = value.split(":");
                    if (bounds.length == 2) {
                        dateHistogram.extendedBounds(new ExtendedBounds(bounds[0], bounds[1]));
                    }
                    break;

                case "alias":
                case "nested":
                case "reverse_nested":
                case "children":
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
            if (kv.key != null && kv.key.equals("alias"))
                alias = kv.value.toString();
        }
        return alias;
    }

    private HistogramAggregationBuilder histogram(MethodField field) throws SqlParseException {
        String aggName = gettAggNameFromParamsOrAlias(field);
        HistogramAggregationBuilder histogram = AggregationBuilders.histogram(aggName);
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
                case "children":
                    break;
                case "order":
                    BucketOrder order = null;
                    switch (value) {
                        case "key_desc":
                            order = BucketOrder.key(false);
                            break;
                        case "count_asc":
                            order = BucketOrder.count(true);
                            break;
                        case "count_desc":
                            order = BucketOrder.count(false);
                            break;
                        case "key_asc":
                        default:
                            order = BucketOrder.key(true);
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
    private RangeAggregationBuilder rangeBuilder(MethodField field) {

        LinkedList<KVValue> params = new LinkedList<>(field.getParams());

        String fieldName = params.poll().toString();

        double[] ds = Util.KV2DoubleArr(params);

        RangeAggregationBuilder range = AggregationBuilders.range(field.getAlias()).field(fieldName);

        for (int i = 1; i < ds.length; i++) {
            range.addRange(ds[i - 1], ds[i]);
        }

        return range;
    }


    /**
     * Create count aggregation.
     *
     * @param field The count function
     * @return AggregationBuilder use to count result
     */
    private ValuesSourceAggregationBuilder makeCountAgg(MethodField field) {

        // Cardinality is approximate DISTINCT.
        if ("DISTINCT".equals(field.getOption())) {

            if (field.getParams().size() == 1) {
                return AggregationBuilders.cardinality(field.getAlias()).field(field.getParams().get(0).value.toString());
            } else {
                Integer precision_threshold = (Integer) (field.getParams().get(1).value);
                return AggregationBuilders.cardinality(field.getAlias()).precisionThreshold(precision_threshold).field(field.getParams().get(0).value.toString());
            }

        }

        String fieldName = field.getParams().get(0).value.toString();

        /*
        zhongshu-comment count(1) count(0)这种应该是查不到东西的，除非你的字段名就叫做1、0这样
            es的count是针对某个字段做count的，见下面的dsl，对os这个字段做count
                "aggregations": {
                    "COUNT(os)": {
                      "value_count": {
                        "field": "os"
                      }
                    }
                 }

            假如你是写count(*)，那es-sql就帮你转成对"_index"字段做count，每一条数据都会有"_index"字段，该字段存储的是索引的名字
         */
        // In case of count(*) we use '_index' as field parameter to count all documents
        if ("*".equals(fieldName)) {
            KVValue kvValue = new KVValue(null, "_index");
            field.getParams().set(0, kvValue);
            /*
            zhongshu-comment 这个看起来有点多此一举：先将"_index"字符串封装到KVValue中，然后再kv.toString()得到"_index"字符串，还不如直接将"_index"传进去AggregationBuilders.count(field.getAlias()).field("_index");
            其实目的是为了改变形参MethodField field的params参数中的值，由"*"改为"_index"
             */
            return AggregationBuilders.count(field.getAlias()).field(kvValue.toString());
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
        String alias = gettAggNameFromParamsOrAlias(field);
        TopHitsAggregationBuilder topHits = AggregationBuilders.topHits(alias);
        List<KVValue> params = field.getParams();
        String[] include = null;
        String[] exclude = null;
        for (KVValue kv : params) {
            switch (kv.key) {
                case "from":
                    topHits.from((int) kv.value);
                    break;
                case "size":
                    topHits.size((int) kv.value);
                    break;
                case "include":
                    include = kv.value.toString().split(",");
                    break;
                case "exclude":
                    exclude = kv.value.toString().split(",");
                    break;
                case "alias":
                case "nested":
                case "reverse_nested":
                case "children":
                    break;
                default:
                    topHits.sort(kv.key, SortOrder.valueOf(kv.value.toString().toUpperCase()));
                    break;
            }
        }
        if (include != null || exclude != null) {
            topHits.fetchSource(include, exclude);
        }
        return topHits;
    }

    public Map<String, KVValue> getGroupMap() {
        return this.groupMap;
    }

}
