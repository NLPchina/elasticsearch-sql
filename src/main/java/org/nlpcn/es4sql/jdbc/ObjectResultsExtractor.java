package org.nlpcn.es4sql.jdbc;

import com.google.common.collect.Maps;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.metrics.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.GeoBounds;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Created by allwefantasy on 8/30/16.
 */
public class ObjectResultsExtractor {
    private final boolean includeType;
    private final boolean includeScore;
    private final boolean includeId;
    private final boolean includeScrollId;
    private int currentLineIndex;
    private QueryAction queryAction;

    public ObjectResultsExtractor(boolean includeScore, boolean includeType, boolean includeId, boolean includeScrollId, QueryAction queryAction) {
        this.includeScore = includeScore;
        this.includeType = includeType;
        this.includeId = includeId;
        this.includeScrollId = includeScrollId;
        this.currentLineIndex = 0;
        this.queryAction = queryAction;
    }

    public ObjectResult extractResults(Object queryResult, boolean flat) throws ObjectResultsExtractException {
        if (queryResult instanceof SearchHits) {
            SearchHit[] hits = ((SearchHits) queryResult).getHits();
            List<Map<String, Object>> docsAsMap = new ArrayList<>();
            Set<String> hitFieldNames = new HashSet<>();
            List<String> headers = createHeadersAndFillDocsMap(flat, hits, null, docsAsMap, hitFieldNames);
            List<List<Object>> lines = createLinesFromDocs(flat, docsAsMap, headers, hitFieldNames);
            return new ObjectResult(headers, lines);
        }
        if (queryResult instanceof Aggregations) {
            List<String> headers = new ArrayList<>();
            List<List<Object>> lines = new ArrayList<>();
            lines.add(new ArrayList<Object>());
            handleAggregations((Aggregations) queryResult, headers, lines);
            
            // remove empty line。
            if(lines.get(0).size() == 0) {
                lines.remove(0);
            }
            //todo: need to handle more options for aggregations:
            //Aggregations that inhrit from base
            //ScriptedMetric

            return new ObjectResult(headers, lines);

        }
        if (queryResult instanceof SearchResponse) {
            SearchHit[] hits = ((SearchResponse) queryResult).getHits().getHits();
            List<Map<String, Object>> docsAsMap = new ArrayList<>();
            Set<String> hitFieldNames = new HashSet<>();
            List<String> headers = createHeadersAndFillDocsMap(flat, hits, ((SearchResponse) queryResult).getScrollId(), docsAsMap, hitFieldNames);
            List<List<Object>> lines = createLinesFromDocs(flat, docsAsMap, headers, hitFieldNames);
            return new ObjectResult(headers, lines);
        }
        return null;
    }

    private void handleAggregations(Aggregations aggregations, List<String> headers, List<List<Object>> lines) throws ObjectResultsExtractException {
        if (allNumericAggregations(aggregations)) {
            lines.get(this.currentLineIndex).addAll(fillHeaderAndCreateLineForNumericAggregations(aggregations, headers));
            return;
        }
        //aggregations with size one only supported when not metrics.
        List<Aggregation> aggregationList = aggregations.asList();
        if (aggregationList.size() > 1) {
            throw new ObjectResultsExtractException("currently support only one aggregation at same level (Except for numeric metrics)");
        }
        Aggregation aggregation = aggregationList.get(0);
        //we want to skip singleBucketAggregations (nested,reverse_nested,filters)
        if (aggregation instanceof SingleBucketAggregation) {
            Aggregations singleBucketAggs = ((SingleBucketAggregation) aggregation).getAggregations();
            handleAggregations(singleBucketAggs, headers, lines);
            return;
        }
        if (aggregation instanceof NumericMetricsAggregation) {
            handleNumericMetricAggregation(headers, lines.get(currentLineIndex), aggregation);
            return;
        }
        if (aggregation instanceof GeoBounds) {
            handleGeoBoundsAggregation(headers, lines, (GeoBounds) aggregation);
            return;
        }
        if (aggregation instanceof TopHits) {
            //todo: handle this . it returns hits... maby back to normal?
            //todo: read about this usages
            // TopHits topHitsAggregation = (TopHits) aggregation;
        }
        if (aggregation instanceof MultiBucketsAggregation) {
            MultiBucketsAggregation bucketsAggregation = (MultiBucketsAggregation) aggregation;
            String name = bucketsAggregation.getName();
            //checking because it can comes from sub aggregation again
            if (!headers.contains(name)) {
                headers.add(name);
            }
            Collection<? extends MultiBucketsAggregation.Bucket> buckets = bucketsAggregation.getBuckets();

            //clone current line.
            List<Object> currentLine = lines.get(this.currentLineIndex);
            List<Object> clonedLine = new ArrayList<>(currentLine);

            //call handle_Agg with current_line++
            boolean firstLine = true;
            for (MultiBucketsAggregation.Bucket bucket : buckets) {
                //each bucket need to add new line with current line copied => except for first line
                String key = bucket.getKeyAsString();
                if (firstLine) {
                    firstLine = false;
                } else {
                    currentLineIndex++;
                    currentLine = new ArrayList<Object>(clonedLine);
                    lines.add(currentLine);
                }
                currentLine.add(key);
                handleAggregations(bucket.getAggregations(), headers, lines);

            }
        }

    }

    private void handleGeoBoundsAggregation(List<String> headers, List<List<Object>> lines, GeoBounds geoBoundsAggregation) {
        String geoBoundAggName = geoBoundsAggregation.getName();
        headers.add(geoBoundAggName + ".topLeft.lon");
        headers.add(geoBoundAggName + ".topLeft.lat");
        headers.add(geoBoundAggName + ".bottomRight.lon");
        headers.add(geoBoundAggName + ".bottomRight.lat");
        List<Object> line = lines.get(this.currentLineIndex);
        line.add(String.valueOf(geoBoundsAggregation.topLeft().getLon()));
        line.add(String.valueOf(geoBoundsAggregation.topLeft().getLat()));
        line.add(String.valueOf(geoBoundsAggregation.bottomRight().getLon()));
        line.add(String.valueOf(geoBoundsAggregation.bottomRight().getLat()));
        lines.add(line);
    }

    private List<Object> fillHeaderAndCreateLineForNumericAggregations(Aggregations aggregations, List<String> header) throws ObjectResultsExtractException {
        List<Object> line = new ArrayList<>();
        List<Aggregation> aggregationList = aggregations.asList();
        for (Aggregation aggregation : aggregationList) {
            handleNumericMetricAggregation(header, line, aggregation);
        }
        return line;
    }

    private void handleNumericMetricAggregation(List<String> header, List<Object> line, Aggregation aggregation) throws ObjectResultsExtractException {
        String name = aggregation.getName();

        if (aggregation instanceof NumericMetricsAggregation.SingleValue) {
            if (!header.contains(name)) {
                header.add(name);
            }
            line.add(((NumericMetricsAggregation.SingleValue) aggregation).value());
        }
        //todo:Numeric MultiValue - Stats,ExtendedStats,Percentile...
        else if (aggregation instanceof NumericMetricsAggregation.MultiValue) {
            if (aggregation instanceof Stats) {
                String[] statsHeaders = new String[]{"count", "sum", "avg", "min", "max"};
                boolean isExtendedStats = aggregation instanceof ExtendedStats;
                if (isExtendedStats) {
                    String[] extendedHeaders = new String[]{"sumOfSquares", "variance", "stdDeviation"};
                    statsHeaders = Util.concatStringsArrays(statsHeaders, extendedHeaders);
                }
                mergeHeadersWithPrefix(header, name, statsHeaders);
                Stats stats = (Stats) aggregation;
                line.add(stats.getCount());
                line.add(stats.getSum());
                line.add(stats.getAvg());
                line.add(stats.getMin());
                line.add(stats.getMax());
                if (isExtendedStats) {
                    ExtendedStats extendedStats = (ExtendedStats) aggregation;
                    line.add(extendedStats.getSumOfSquares());
                    line.add(extendedStats.getVariance());
                    line.add(extendedStats.getStdDeviation());
                }
            } else if (aggregation instanceof Percentiles) {
                String[] percentileHeaders = new String[]{"1.0", "5.0", "25.0", "50.0", "75.0", "95.0", "99.0"};
                mergeHeadersWithPrefix(header, name, percentileHeaders);
                Percentiles percentiles = (Percentiles) aggregation;
                line.add(percentiles.percentile(1.0));
                line.add(percentiles.percentile(5.0));
                line.add(percentiles.percentile(25.0));
                line.add(percentiles.percentile(50.0));
                line.add(percentiles.percentile(75));
                line.add(percentiles.percentile(95.0));
                line.add(percentiles.percentile(99.0));
            } else {
                throw new ObjectResultsExtractException("unknown NumericMetricsAggregation.MultiValue:" + aggregation.getClass());
            }

        } else {
            throw new ObjectResultsExtractException("unknown NumericMetricsAggregation" + aggregation.getClass());
        }
    }

    private void mergeHeadersWithPrefix(List<String> header, String prefix, String[] newHeaders) {
        for (int i = 0; i < newHeaders.length; i++) {
            String newHeader = newHeaders[i];
            if (prefix != null && !prefix.equals("")) {
                newHeader = prefix + "." + newHeader;
            }
            if (!header.contains(newHeader)) {
                header.add(newHeader);
            }
        }
    }

    private boolean allNumericAggregations(Aggregations aggregations) {
        List<Aggregation> aggregationList = aggregations.asList();
        for (Aggregation aggregation : aggregationList) {
            if (!(aggregation instanceof NumericMetricsAggregation)) {
                return false;
            }
        }
        return true;
    }

    private Aggregation skipAggregations(Aggregation firstAggregation) {
        while (firstAggregation instanceof SingleBucketAggregation) {
            firstAggregation = getFirstAggregation(((SingleBucketAggregation) firstAggregation).getAggregations());
        }
        return firstAggregation;
    }

    private Aggregation getFirstAggregation(Aggregations aggregations) {
        return aggregations.asList().get(0);
    }

    private List<List<Object>> createLinesFromDocs(boolean flat, List<Map<String, Object>> docsAsMap, List<String> headers, Set<String> hitFieldNames) {
        List<List<Object>> objectLines = new ArrayList<>();
        for (Map<String, Object> doc : docsAsMap) {
            List<Object> lines = new ArrayList<>();
            for (String header : headers) {
                lines.add(findFieldValue(header, doc, flat, hitFieldNames));
            }
            objectLines.add(lines);
        }
        return objectLines;
    }

    private List<String> createHeadersAndFillDocsMap(boolean flat, SearchHit[] hits, String scrollId, List<Map<String, Object>> docsAsMap, Set<String> hitFieldNames) {
        Set<String> headers = new LinkedHashSet<>();
        List<String> fieldNames = new ArrayList<>();
        if (this.queryAction instanceof DefaultQueryAction) {
            fieldNames.addAll(((DefaultQueryAction) this.queryAction).getFieldNames());
        }
        boolean hasScrollId = this.includeScrollId || fieldNames.contains("_scroll_id");
        for (SearchHit hit : hits) {
            Map<String, Object> doc = Optional.ofNullable(hit.getSourceAsMap()).orElse(Maps.newHashMap());
            if (this.includeScore) {
                doc.put("_score", hit.getScore());
            }
            if (this.includeType) {
                doc.put("_type", hit.getType());
            }
            if (this.includeId) {
                doc.put("_id", hit.getId());
            }
            if (hasScrollId) {
                doc.put("_scroll_id", scrollId);
            }
            mergeHeaders(headers, doc, flat);

            // hit fields
            Map<String, DocumentField> fields = hit.getFields();
            for (DocumentField searchHitField : fields.values()) {
                List<Object> values = Optional.ofNullable(searchHitField.getValues()).orElse(Collections.emptyList());
                int size = values.size();
                doc.put(searchHitField.getName(), size == 1 ? values.get(0) : size > 1 ? values : null);
                hitFieldNames.add(searchHitField.getName());
                headers.add(searchHitField.getName());
            }

            docsAsMap.add(doc);
        }
        List<String> list = new ArrayList<>(headers);
        if (!fieldNames.isEmpty()) {
            list.sort((o1, o2) -> {
                int i1 = fieldNames.indexOf(o1);
                int i2 = fieldNames.indexOf(o2);
                return Integer.compare(i1 < 0 ? Integer.MAX_VALUE : i1, i2 < 0 ? Integer.MAX_VALUE : i2);
            });
        }
        return list;
    }

    private Object findFieldValue(String header, Map<String, Object> doc, boolean flat, Set<String> hitFieldNames) {
        if (flat && header.contains(".") && !hitFieldNames.contains(header)) {
            String[] split = header.split("\\.");
            Object innerDoc = doc;
            for (String innerField : split) {
                if (!(innerDoc instanceof Map)) {
                    return null;
                }
                innerDoc = ((Map<String, Object>) innerDoc).get(innerField);
                if (innerDoc == null) {
                    return null;
                }

            }
            return innerDoc;
        } else {
            if (doc.containsKey(header)) {
                return doc.get(header);
            }
        }
        return null;
    }

    private void mergeHeaders(Set<String> headers, Map<String, Object> doc, boolean flat) {
        if (!flat) {
            headers.addAll(doc.keySet());
            return;
        }
        mergeFieldNamesRecursive(headers, doc, "");
    }

    private void mergeFieldNamesRecursive(Set<String> headers, Map<String, Object> doc, String prefix) {
        for (Map.Entry<String, Object> field : doc.entrySet()) {
            Object value = field.getValue();
            if (value instanceof Map) {
                mergeFieldNamesRecursive(headers, (Map<String, Object>) value, prefix + field.getKey() + ".");
            } else {
                headers.add(prefix + field.getKey());
            }
        }
    }
}
