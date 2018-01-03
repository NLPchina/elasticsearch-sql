package org.elasticsearch.plugin.nlpcn.executors;

import com.google.common.base.Joiner;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBounds;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.nlpcn.es4sql.Util;

import java.util.*;

/**
 * Created by Eliran on 27/12/2015.
 */
public class CSVResultsExtractor {
    private final boolean includeType;
    private final boolean includeScore;
    private final boolean indcludeId;
    private int currentLineIndex;
    public CSVResultsExtractor(boolean includeScore, boolean includeType, boolean includeId) {
        this.includeScore = includeScore;
        this.includeType = includeType;
        this.indcludeId = includeId;
        this.currentLineIndex = 0;
    }

    public CSVResult extractResults(Object queryResult, boolean flat, String separator) throws CsvExtractorException {
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
            //Aggregations that inhrit from base
            //ScriptedMetric

            return new CSVResult(headers,csvLines);

        }
        return null;
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
            line.add(((NumericMetricsAggregation.SingleValue) aggregation).getValueAsString());
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
                String[] percentileHeaders = new String[]{"1.0", "5.0", "25.0", "50.0", "75.0", "95.0", "99.0"};
                mergeHeadersWithPrefix(header, name, percentileHeaders);
                Percentiles percentiles = (Percentiles) aggregation;
                line.add(percentiles.percentileAsString(1.0));
                line.add(percentiles.percentileAsString(5.0));
                line.add(percentiles.percentileAsString(25.0));
                line.add(percentiles.percentileAsString(50.0));
                line.add(percentiles.percentileAsString(75));
                line.add(percentiles.percentileAsString(95.0));
                line.add(percentiles.percentileAsString(99.0));
            }
            else {
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

    private List<String> createCSVLinesFromDocs(boolean flat, String separator, List<Map<String, Object>> docsAsMap, List<String> headers) {
        List<String> csvLines = new ArrayList<>();
        for(Map<String,Object> doc : docsAsMap){
            String line = "";
            for(String header : headers){
                line += findFieldValue(header, doc, flat, separator);
            }
            csvLines.add(line.substring(0, line.lastIndexOf(separator)));
        }
        return csvLines;
    }

    private List<String> createHeadersAndFillDocsMap(boolean flat, SearchHit[] hits, List<Map<String, Object>> docsAsMap) {
        Set<String> csvHeaders = new HashSet<>();
        for(SearchHit hit : hits){
            Map<String, Object> doc = hit.getSourceAsMap();
            Map<String, DocumentField> fields = hit.getFields();
            for(DocumentField searchHitField : fields.values()){
                doc.put(searchHitField.getName(),searchHitField.getValue());
            }
            mergeHeaders(csvHeaders, doc, flat);
            if(this.indcludeId){
                doc.put("_id", hit.getId());
            }
            if(this.includeScore){
                doc.put("_score", hit.getScore());
            }
            if(this.includeType){
                doc.put("_type",hit.getType());
            }
            docsAsMap.add(doc);
        }
        ArrayList<String> headersList = new ArrayList<>(csvHeaders);
        if (this.indcludeId){
            headersList.add("_id");
        }
        if (this.includeScore){
            headersList.add("_score");
        }
        if (this.includeType){
            headersList.add("_type");
        }
        return headersList;
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
                return String.valueOf(doc.get(header)) + separator;
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
