package org.elasticsearch.plugin.nlpcn.executors;

import com.sun.org.apache.xpath.internal.operations.Mult;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.base.Joiner;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGrid;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.scripted.ScriptedMetric;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;

import java.util.*;

/**
 * Created by Eliran on 27/12/2015.
 */
public class CSVResultsExtractor {
    private int currentLineIndex;

    public CSVResultsExtractor() {
        this.currentLineIndex = 0;
    }

    public CSVResult extractResults(Object queryResult, boolean flat, String separator) {
        if(queryResult instanceof SearchHits){
            SearchHit[] hits = ((SearchHits) queryResult).getHits();
            List<Map<String,Object>> docsAsMap = new ArrayList<>();
            List<String> headers = createHeadersAndFillDocsMap(flat, hits, docsAsMap);
            List<String> csvLines = createCSVLinesFromDocs(flat, separator, docsAsMap, headers);
            return new CSVResult(headers,csvLines);
        }
        if(queryResult instanceof Aggregations){
            List<String> headers = new ArrayList<>();
            List<List<String>> lines = new ArrayList<>();
            lines.add(new ArrayList<String>());
            handleAggregations((Aggregations) queryResult, headers, lines);

            List<String> csvLines  = new ArrayList<>();
            for(List<String> simpleLine : lines){
                csvLines.add(Joiner.on(separator).join(simpleLine));
            }

            //todo: need to handle more options for aggregations:
            //NumericMetricsAggregation.Multi : ExtendedStats,Stats,Percentiles
            //Aggregations that inhrit from base
            //ScriptedMetric
            //TopHits
            //GeoBounds

            return new CSVResult(headers,csvLines);

        }
        return null;
    }

    private  void handleAggregations(Aggregations aggregations, List<String> headers, List<List<String>> lines) {
        if(allNumericAggregations(aggregations)){
            lines.get(this.currentLineIndex).addAll(fillHeaderAndCreateLineForNumericAggregations(aggregations, headers));
            return;
        }
        //aggregations with size one only supported when not metrics.
        List<Aggregation> aggregationList = aggregations.asList();
        if(aggregationList.size() > 1){
            //todo: throw exception
        }
        Aggregation aggregation = aggregationList.get(0);
        //we want to skip singleBucketAggregations (nested,reverse_nested,filters)
        if(aggregation instanceof SingleBucketAggregation){
            Aggregations singleBucketAggs = ((SingleBucketAggregation) aggregation).getAggregations();
            handleAggregations(singleBucketAggs,headers,lines);
            return;
        }
        if(aggregation instanceof NumericMetricsAggregation){
            handleNumericMetricAggregation(headers,lines.get(currentLineIndex),aggregation);
            return;
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
                String key = bucket.getKeyAsText().string();
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

    private  List<String> fillHeaderAndCreateLineForNumericAggregations(Aggregations aggregations, List<String> header) {
        List<String> line = new ArrayList<>();
        List<Aggregation> aggregationList = aggregations.asList();
        for(Aggregation aggregation : aggregationList){
            handleNumericMetricAggregation(header, line, aggregation);
        }
        return line;
    }

    private  void handleNumericMetricAggregation(List<String> header, List<String> line, Aggregation aggregation) {
        String name = aggregation.getName();
        if(!header.contains(name)){
            header.add(aggregation.getName());
        }
        if(aggregation instanceof NumericMetricsAggregation.SingleValue){
            line.add(((NumericMetricsAggregation.SingleValue) aggregation).getValueAsString());
        }
        //todo:Numeric MultiValue - Stats,ExtendedStats,Percentile...
        else {

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

    private List<String> createCSVLinesFromDocs(boolean flat, String separator, List<Map<String, Object>> docsAsMap, List<String> headers) {
        List<String> csvLines = new ArrayList<>();
        for(Map<String,Object> doc : docsAsMap){
            String line = "";
            for(String header : headers){
                line += findFieldValue(header, doc, flat, separator);
            }
            csvLines.add(line.substring(0, line.length() - 1));
        }
        return csvLines;
    }

    private List<String> createHeadersAndFillDocsMap(boolean flat, SearchHit[] hits, List<Map<String, Object>> docsAsMap) {
        Set<String> csvHeaders = new HashSet<>();
        for(SearchHit hit : hits){
            Map<String, Object> doc = hit.sourceAsMap();
            mergeHeaders(csvHeaders,doc,flat);
            docsAsMap.add(doc);
        }
        return new ArrayList<>(csvHeaders);
    }

    private String findFieldValue(String header, Map<String, Object> doc, boolean flat, String separator) {
        if(flat && header.contains(".")){
            String[] split = header.split("\\.");
            Object innerDoc = doc;
            for(String innerField : split){
                if(!(innerDoc instanceof Map)){
                    return separator;
                }
                innerDoc = ((Map<String,Object>)innerDoc).get(innerField);
                if(innerDoc == null){
                    return separator;
                }

            }
            return innerDoc.toString() + separator;
        }
        else {
            if(doc.containsKey(header)){
                return doc.get(header).toString() + separator;
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
