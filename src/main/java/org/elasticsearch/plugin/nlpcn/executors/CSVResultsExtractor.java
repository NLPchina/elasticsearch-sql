package org.elasticsearch.plugin.nlpcn.executors;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.util.*;

/**
 * Created by Eliran on 27/12/2015.
 */
public class CSVResultsExtractor {
    public static CSVResult extractResults(Object queryResult, boolean flat, String separator) {
        if(queryResult instanceof SearchHits){
            SearchHit[] hits = ((SearchHits) queryResult).getHits();
            Set<String> csvHeaders = new HashSet<>();
            List<Map<String,Object>> docsAsMap = new ArrayList<>();
            for(SearchHit hit : hits){
                Map<String, Object> doc = hit.sourceAsMap();
                mergeHeaders(csvHeaders,doc,flat);
                docsAsMap.add(doc);
            }
            List<String> headers = new ArrayList<>(csvHeaders);

            List<String> csvLines = new ArrayList<>();
            for(Map<String,Object> doc : docsAsMap){
                String line = "";
                for(String header : headers){
                    line += findFieldValue(header, doc, flat, separator);
                }
                csvLines.add(line.substring(0, line.length() - 1));
            }

            return new CSVResult(headers,csvLines);
        }
        return null;
    }

    private static String findFieldValue(String header, Map<String, Object> doc, boolean flat, String separator) {
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

    private static void mergeHeaders(Set<String> headers, Map<String, Object> doc, boolean flat) {
        if (!flat) {
            headers.addAll(doc.keySet());
            return;
        }
        mergeFieldNamesRecursive(headers, doc, "");
    }

    private static void mergeFieldNamesRecursive(Set<String> headers, Map<String, Object> doc, String prefix) {
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
