package org.elasticsearch.plugin.nlpcn.executors;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.metrics.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.GeoBounds;
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.Percentile;
import org.elasticsearch.search.aggregations.metrics.Percentiles;
import org.elasticsearch.search.aggregations.metrics.Stats;
import org.elasticsearch.search.aggregations.metrics.TopHits;
import org.nlpcn.es4sql.Util;
import org.nlpcn.es4sql.query.DefaultQueryAction;
import org.nlpcn.es4sql.query.QueryAction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by Eliran on 27/12/2015.
 */
public class CSVResultsExtractor {
    private final boolean includeType;
    private final boolean includeScore;
    private final boolean includeId;
    private final boolean includeScrollId;
    private boolean includeIndex;
    private int currentLineIndex;
    private QueryAction queryAction;

    public CSVResultsExtractor(boolean includeScore, boolean includeType, boolean includeId, boolean includeScrollId, QueryAction queryAction) {
        this.includeScore = includeScore;
        this.includeType = includeType;
        this.includeId = includeId;
        this.includeScrollId = includeScrollId;
        this.currentLineIndex = 0;
        this.queryAction = queryAction;
    }

    public CSVResultsExtractor(boolean includeIndex, boolean includeScore, boolean includeType, boolean includeId, boolean includeScrollId, QueryAction queryAction) {
        this.includeIndex = includeIndex;
        this.includeScore = includeScore;
        this.includeType = includeType;
        this.includeId = includeId;
        this.includeScrollId = includeScrollId;
        this.currentLineIndex = 0;
        this.queryAction = queryAction;
    }


    public CSVResult extractResults(Object queryResult, boolean flat, String separator, boolean quote) throws CsvExtractorException {
        if(queryResult instanceof SearchHits){
            SearchHit[] hits = ((SearchHits) queryResult).getHits();
            List<Map<String,Object>> docsAsMap = new ArrayList<>();
            Set<String> hitFieldNames = new HashSet<>();
            List<String> headers = createHeadersAndFillDocsMap(flat, hits, null, docsAsMap, hitFieldNames);
            List<String> csvLines = createCSVLinesFromDocs(flat, separator, quote, docsAsMap, headers, hitFieldNames);
            return new CSVResult(headers,csvLines);
        }
        if(queryResult instanceof Aggregations){
            List<String> headers = new ArrayList<>();
            List<List<String>> lines = new ArrayList<>();
            lines.add(new ArrayList<String>());
            handleAggregations((Aggregations) queryResult, headers, lines);

            List<String> csvLines  = new ArrayList<>();
            for(List<String> simpleLine : lines){
                csvLines.add(Joiner.on(separator).join(quote ? simpleLine.stream().map(Util::quoteString).collect(Collectors.toList()) : simpleLine));
            }

            //todo: need to handle more options for aggregations:
            //Aggregations that inhrit from base
            //ScriptedMetric

            return new CSVResult(headers,csvLines);

        }
        if (queryResult instanceof SearchResponse) {
            SearchHit[] hits = ((SearchResponse) queryResult).getHits().getHits();
            List<Map<String, Object>> docsAsMap = new ArrayList<>();
            Set<String> hitFieldNames = new HashSet<>();
            List<String> headers = createHeadersAndFillDocsMap(flat, hits, ((SearchResponse) queryResult).getScrollId(), docsAsMap, hitFieldNames);
            List<String> csvLines = createCSVLinesFromDocs(flat, separator, quote, docsAsMap, headers, hitFieldNames);
            //return new CSVResult(headers, csvLines);
            return new CSVResult(headers, csvLines, ((SearchResponse) queryResult).getHits().getTotalHits().value);
        }
        if (queryResult instanceof GetIndexResponse){
            ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetadata>> mappings = ((GetIndexResponse) queryResult).getMappings();
            List<String> headers = Lists.newArrayList("field", "type");
            List<String> csvLines  = new ArrayList<>();
            List<List<String>> lines = new ArrayList<>();
            Iterator<String> iter = mappings.keysIt();
            while (iter.hasNext()) {
                String index = iter.next();
                MappingMetadata mappingJson = (MappingMetadata)mappings.get(index).values().toArray()[0];
                 LinkedHashMap properties = (LinkedHashMap) mappingJson.sourceAsMap().get("properties");
                Map<Object, Object> mapping = Maps.newLinkedHashMap();
                parseMapping(Lists.newArrayList(), properties, mapping, 0);
                for (Object key : mapping.keySet()) {
                    lines.add(Lists.newArrayList(key.toString(), mapping.get(key).toString()));
                }
            }

            for(List<String> simpleLine : lines){
                csvLines.add(Joiner.on(separator).join(simpleLine));
            }

              return new CSVResult(headers, csvLines, csvLines.size());
        }


        return null;
    }

    private static void parseMapping(ArrayList path, LinkedHashMap properties, Map<Object, Object> mapping, int children) {
        int passed = 1;
        for (Object key : properties.keySet()) {
            if (properties.get(key) instanceof LinkedHashMap) {
                LinkedHashMap value = (LinkedHashMap) properties.get(key);
                if (!key.equals("properties")) {
                    path.add(key.toString());
                }
                if (value.containsKey("type")) {
                    String realPath = parsePath(path.toString());
                    mapping.put(realPath , value.get("type"));
                    if (value.containsKey("fields")) {
                        mapping.put(realPath + ".keyword", "keyword");
                    }
                    if (passed == children) {
                        if (path.size() - 2 >= 0) {//还要清理当前key的上层
                            path.remove(path.size() - 2);
                        }
                    }
                    path.remove(path.size() - 1);//移除当前元素
                } else {
                    if (value.containsKey("properties")) {
                        children = ((LinkedHashMap) value.get("properties")).size();
                    }
                    parseMapping(path, value, mapping, children);
                }
            }
            passed++;
        }
    }

    private  static String parsePath(String path) {
        return path.replaceAll("\\s+", "").replace("[", "").replace("]", "").replace(",", ".");
    }

    private  void handleAggregations(Aggregations aggregations, List<String> headers, List<List<String>> lines) throws CsvExtractorException {
        if(allNumericAggregations(aggregations)){
            lines.get(this.currentLineIndex).addAll(fillHeaderAndCreateLineForNumericAggregations(aggregations, headers));
            return;
        }
        //aggregations with size one only supported when not metrics.
        List<Aggregation> aggregationList = aggregations.asList();
        if(aggregationList.size() > 1){
            throw new CsvExtractorException("currently support only one aggregation at same level (Except for numeric metrics)");
        }
        Aggregation aggregation = aggregationList.get(0);
        //we want to skip singleBucketAggregations (nested,reverse_nested,filters)
        if(aggregation instanceof SingleBucketAggregation){
            Aggregations singleBucketAggs = ((SingleBucketAggregation) aggregation).getAggregations();
            handleAggregations(singleBucketAggs, headers, lines);
            return;
        }
        if(aggregation instanceof NumericMetricsAggregation){
            handleNumericMetricAggregation(headers, lines.get(currentLineIndex), aggregation);
            return;
        }
        if(aggregation instanceof GeoBounds){
            handleGeoBoundsAggregation(headers, lines, (GeoBounds) aggregation);
            return;
        }
        if(aggregation instanceof TopHits){
            //todo: handle this . it returns hits... maby back to normal?
            //todo: read about this usages
            // TopHits topHitsAggregation = (TopHits) aggregation;
        }
        if(aggregation instanceof MultiBucketsAggregation){
            MultiBucketsAggregation bucketsAggregation = (MultiBucketsAggregation) aggregation;
            String name = bucketsAggregation.getName();
            //checking because it can comes from sub aggregation again
            if(!headers.contains(name)){
                headers.add(name);
            }
            Collection<? extends MultiBucketsAggregation.Bucket> buckets = bucketsAggregation.getBuckets();

            //clone current line.
            List<String> currentLine = lines.get(this.currentLineIndex);
            List<String> clonedLine = new ArrayList<>(currentLine);

            //call handle_Agg with current_line++
            boolean firstLine = true;
            for (MultiBucketsAggregation.Bucket bucket : buckets) {
                //each bucket need to add new line with current line copied => except for first line
                String key = bucket.getKeyAsString();
                if(firstLine){
                    firstLine = false;
                }
                else {
                    currentLineIndex++;
                    currentLine = new ArrayList<String>(clonedLine);
                    lines.add(currentLine);
                }
                currentLine.add(key);
                handleAggregations(bucket.getAggregations(),headers,lines);

            }
        }

    }

    private void handleGeoBoundsAggregation(List<String> headers, List<List<String>> lines, GeoBounds geoBoundsAggregation) {
        String geoBoundAggName = geoBoundsAggregation.getName();
        headers.add(geoBoundAggName+".topLeft.lon");
        headers.add(geoBoundAggName+".topLeft.lat");
        headers.add(geoBoundAggName+".bottomRight.lon");
        headers.add(geoBoundAggName+".bottomRight.lat");
        List<String> line = lines.get(this.currentLineIndex);
        line.add(String.valueOf(geoBoundsAggregation.topLeft().getLon()));
        line.add(String.valueOf(geoBoundsAggregation.topLeft().getLat()));
        line.add(String.valueOf(geoBoundsAggregation.bottomRight().getLon()));
        line.add(String.valueOf(geoBoundsAggregation.bottomRight().getLat()));
        lines.add(line);
    }

    private  List<String> fillHeaderAndCreateLineForNumericAggregations(Aggregations aggregations, List<String> header) throws CsvExtractorException {
        List<String> line = new ArrayList<>();
        List<Aggregation> aggregationList = aggregations.asList();
        for(Aggregation aggregation : aggregationList){
            handleNumericMetricAggregation(header, line, aggregation);
        }
        return line;
    }

    private  void handleNumericMetricAggregation(List<String> header, List<String> line, Aggregation aggregation) throws CsvExtractorException {
        String name = aggregation.getName();

        if(aggregation instanceof NumericMetricsAggregation.SingleValue){
            if(!header.contains(name)){
                header.add(name);
            }
            NumericMetricsAggregation.SingleValue agg = (NumericMetricsAggregation.SingleValue) aggregation;
            line.add(!Double.isInfinite(agg.value()) ? agg.getValueAsString() : "null");
        }
        //todo:Numeric MultiValue - Stats,ExtendedStats,Percentile...
        else if(aggregation instanceof NumericMetricsAggregation.MultiValue){
            if(aggregation instanceof Stats) {
                String[] statsHeaders = new String[]{"count", "sum", "avg", "min", "max"};
                boolean isExtendedStats = aggregation instanceof ExtendedStats;
                if(isExtendedStats){
                    String[] extendedHeaders = new String[]{"sumOfSquares", "variance", "stdDeviation"};
                    statsHeaders = Util.concatStringsArrays(statsHeaders,extendedHeaders);
                }
                mergeHeadersWithPrefix(header, name, statsHeaders);
                Stats stats = (Stats) aggregation;
                line.add(String.valueOf(stats.getCount()));
                line.add(stats.getSumAsString());
                line.add(stats.getAvgAsString());
                line.add(stats.getMinAsString());
                line.add(stats.getMaxAsString());
                if(isExtendedStats){
                    ExtendedStats extendedStats = (ExtendedStats) aggregation;
                    line.add(extendedStats.getSumOfSquaresAsString());
                    line.add(extendedStats.getVarianceAsString());
                    line.add(extendedStats.getStdDeviationAsString());
                }
            }
            else if( aggregation instanceof Percentiles){
                List<String> percentileHeaders = new ArrayList<>(7);
                Percentiles percentiles = (Percentiles) aggregation;
                for (Percentile p : percentiles) {
                    percentileHeaders.add(String.valueOf(p.getPercent()));
                    line.add(percentiles.percentileAsString(p.getPercent()));
                }
                mergeHeadersWithPrefix(header, name, percentileHeaders.toArray(new String[0]));
            } else if (aggregation instanceof InternalTDigestPercentileRanks) {//added by xzb 增加PercentileRanks函数支持
                InternalTDigestPercentileRanks percentileRanks = (InternalTDigestPercentileRanks) aggregation;
                List<String> percentileHeaders = new ArrayList<>(7);
                for (Percentile rank : percentileRanks) {
                    percentileHeaders.add(String.valueOf(rank.getValue()));
                    line.add(String.valueOf(rank.getPercent()));
                }
                mergeHeadersWithPrefix(header, name, percentileHeaders.toArray(new String[0]));
            } else {
                throw new CsvExtractorException("unknown NumericMetricsAggregation.MultiValue:" + aggregation.getClass());
            }

        }
        else {
            throw new CsvExtractorException("unknown NumericMetricsAggregation" + aggregation.getClass());
        }
    }

    private void mergeHeadersWithPrefix(List<String> header, String prefix, String[] newHeaders) {
        for (int i = 0; i < newHeaders.length; i++) {
            String newHeader = newHeaders[i];
            if(prefix != null && !prefix.equals("")) {
                newHeader = prefix + "." + newHeader;
            }
            if (!header.contains(newHeader)) {
                header.add(newHeader);
            }
        }
    }

    private  boolean allNumericAggregations(Aggregations aggregations) {
        List<Aggregation> aggregationList = aggregations.asList();
        for(Aggregation aggregation : aggregationList){
            if(!(aggregation instanceof NumericMetricsAggregation)){
                return false;
            }
        }
        return true;
    }

    private  Aggregation skipAggregations(Aggregation firstAggregation) {
        while(firstAggregation instanceof SingleBucketAggregation){
            firstAggregation = getFirstAggregation(((SingleBucketAggregation) firstAggregation).getAggregations());
        }
        return firstAggregation;
    }

    private Aggregation getFirstAggregation(Aggregations aggregations){
        return aggregations.asList().get(0);
    }

    private List<String> createCSVLinesFromDocs(boolean flat, String separator, boolean quote, List<Map<String, Object>> docsAsMap, List<String> headers, Set<String> hitFieldNames) {
        List<String> csvLines = new ArrayList<>();
        for(Map<String,Object> doc : docsAsMap){
            String line = "";
            for(String header : headers){
                line += findFieldValue(header, doc, flat, separator, quote, hitFieldNames);
            }
            csvLines.add(line.substring(0, line.lastIndexOf(separator)));
        }
        return csvLines;
    }

    private List<String> createHeadersAndFillDocsMap(boolean flat, SearchHit[] hits, String scrollId, List<Map<String, Object>> docsAsMap, Set<String> hitFieldNames) {
        Set<String> csvHeaders = new LinkedHashSet<>();
        Map<String, String> highlightMap = Maps.newHashMap();
        for (SearchHit hit : hits) {
            //获取高亮内容
            hit.getHighlightFields().forEach((key, value) -> {
                String frag = value.getFragments()[0].toString();
                highlightMap.put(key, frag);
            });

            Map<String, Object> doc = Optional.ofNullable(hit.getSourceAsMap()).orElse(Maps.newHashMap());
            //替换掉将原始结果中字段的值替换为高亮后的内容
            for (Map.Entry<String, Object> entry : doc.entrySet()) {
                if(highlightMap.containsKey(entry.getKey())) {
                    doc.put(entry.getKey(), highlightMap.get(entry.getKey()));
                }
            }

            mergeHeaders(csvHeaders, doc, flat);
            // hit fields
            Map<String, DocumentField> fields = hit.getFields();
            for (DocumentField searchHitField : fields.values()) {
                List<Object> values = Optional.ofNullable(searchHitField.getValues()).orElse(Collections.emptyList());
                int size = values.size();
                doc.put(searchHitField.getName(), size == 1 ? values.get(0) : size > 1 ? values : null);
                hitFieldNames.add(searchHitField.getName());
                csvHeaders.add(searchHitField.getName());
            }
            if (this.includeIndex) {
                doc.put("_index", hit.getIndex());
            }
            if (this.includeId) {
                doc.put("_id", hit.getId());
            }
            if (this.includeScore) {
                doc.put("_score", hit.getScore());
            }
            if (this.includeType) {
                doc.put("_type", hit.getType());
            }
            if (this.includeScrollId) {
                doc.put("_scroll_id", scrollId);
            }
            docsAsMap.add(doc);
        }
        if (this.includeIndex) {
            csvHeaders.add("_index");
        }
        if (this.includeId) {
            csvHeaders.add("_id");
        }
        if (this.includeScore) {
            csvHeaders.add("_score");
        }
        if (this.includeType) {
            csvHeaders.add("_type");
        }
        if (this.includeScrollId) {
            csvHeaders.add("_scroll_id");
        }
        List<String> headers = new ArrayList<>(csvHeaders);
        if (this.queryAction instanceof DefaultQueryAction) {
            List<String> fieldNames = ((DefaultQueryAction) this.queryAction).getFieldNames();
            headers.sort((o1, o2) -> {
                int i1 = fieldNames.indexOf(o1);
                int i2 = fieldNames.indexOf(o2);
                return Integer.compare(i1 < 0 ? Integer.MAX_VALUE : i1, i2 < 0 ? Integer.MAX_VALUE : i2);
            });
        }
        return headers;
    }

    private String findFieldValue(String header, Map<String, Object> doc, boolean flat, String separator, boolean quote, Set<String> hitFieldNames) {
        if(flat && header.contains(".") && !hitFieldNames.contains(header)) {
            String[] split = header.split("\\.");
            Object innerDoc = doc;
            for(String innerField : split){
                if(!(innerDoc instanceof Map)){
                    return separator;
                }
                innerDoc = ((Map<?, ?>) innerDoc).get(innerField);
                if(innerDoc == null){
                    return separator;
                }

            }
            return (quote ? Util.quoteString(innerDoc.toString()) : innerDoc.toString()) + separator;
        }
        else {
            if(doc.containsKey(header)){
                return (quote ? Util.quoteString(String.valueOf(doc.get(header))) : doc.get(header)) + separator;
            }
        }
        return separator;
    }

    private void mergeHeaders(Set<String> headers, Map<String, Object> doc, boolean flat) {
        if (!flat) {
            headers.addAll(doc.keySet());
            return;
        }
        mergeFieldNamesRecursive(headers, doc, "");
    }

    private void mergeFieldNamesRecursive(Set<String> headers, Map<String, Object> doc, String prefix) {
        for(Map.Entry<String,Object> field : doc.entrySet()){
            Object value = field.getValue();
            if(value instanceof Map){
                mergeFieldNamesRecursive(headers,(Map<String,Object>) value,prefix+field.getKey()+".");
            }
            else {
                headers.add(prefix+field.getKey());
            }
        }
    }
}
