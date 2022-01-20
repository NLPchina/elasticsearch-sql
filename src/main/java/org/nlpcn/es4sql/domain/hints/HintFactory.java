package org.nlpcn.es4sql.domain.hints;


import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.yaml.YamlXContentParser;
import org.nlpcn.es4sql.exception.SqlParseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Eliran on 5/9/2015.
 */
public class HintFactory {

    public static Hint getHintFromString(String hintAsString) throws SqlParseException {
        if(hintAsString.startsWith("! USE_NESTED_LOOPS") || hintAsString.startsWith("! USE_NL")){
            return new Hint(HintType.USE_NESTED_LOOPS,null);
        }

        if(hintAsString.startsWith("! SHARD_SIZE")){
            String[] numbers =  getParamsFromHint(hintAsString, "! SHARD_SIZE");
            //todo: check if numbers etc..
            List<Object> params = new ArrayList<>();
            for (String number : numbers){
                if(number.equals("null") || number.equals("infinity")){
                    params.add(null);
                }
                else {
                    params.add(Integer.parseInt(number));
                }
            }
            return new Hint(HintType.SHARD_SIZE,params.toArray());
        }

        if(hintAsString.equals("! HASH_WITH_TERMS_FILTER"))
            return new Hint(HintType.HASH_WITH_TERMS_FILTER,null);
        if(hintAsString.startsWith("! JOIN_TABLES_LIMIT")){
            String[] numbers =  getParamsFromHint(hintAsString, "! JOIN_TABLES_LIMIT");
            //todo: check if numbers etc..
            List<Object> params = new ArrayList<>();
            for (String number : numbers){
                if(number.equals("null") || number.equals("infinity")){
                    params.add(null);
                }
                else {
                    params.add(Integer.parseInt(number));
                }
            }

            return new Hint(HintType.JOIN_LIMIT,params.toArray());
        }
        if(hintAsString.startsWith("! NL_MULTISEARCH_SIZE")) {
            String[] number = getParamsFromHint(hintAsString,"! NL_MULTISEARCH_SIZE");
            //todo: check if numbers etc..
            int multiSearchSize = Integer.parseInt(number[0]);
            return new Hint(HintType.NL_MULTISEARCH_SIZE,new Object[]{multiSearchSize});
        }
        if (hintAsString.startsWith("! USE_SCROLL")) {
            String[] scrollParams = getParamsFromHint(hintAsString, "! USE_SCROLL");
            if (scrollParams != null && scrollParams.length == 2) {
                String param = scrollParams[0];
                return new Hint(HintType.USE_SCROLL,
                        new Object[]{(param.startsWith("\"") && param.endsWith("\"")) || (param.startsWith("'") && param.endsWith("'")) ? param.substring(1, param.length() - 1) :
                                Integer.parseInt(param),
                                Integer.parseInt(scrollParams[1])});
            } else {
                return new Hint(HintType.USE_SCROLL, new Object[]{50, 60000});
            }
        }
        if(hintAsString.startsWith("! IGNORE_UNAVAILABLE")){
            return new Hint(HintType.IGNORE_UNAVAILABLE,null);
        }
        if(hintAsString.startsWith("! DOCS_WITH_AGGREGATION")) {
            Integer[] params = parseParamsAsInts(hintAsString,"! DOCS_WITH_AGGREGATION");
            return new Hint(HintType.DOCS_WITH_AGGREGATION, params);
        }
        if(hintAsString.startsWith("! ROUTINGS")) {
            String[] routings = getParamsFromHint(hintAsString,"! ROUTINGS");
            return new Hint(HintType.ROUTINGS,routings);
        }
        if(hintAsString.startsWith("! HIGHLIGHT")) {
            String[] heighlights = getParamsFromHint(hintAsString,"! HIGHLIGHT");
            ArrayList hintParams = new ArrayList();
            hintParams.add(heighlights[0]);
            if(heighlights.length > 1 ){
                StringBuilder builder = new StringBuilder();
                for(int i=1;i<heighlights.length;i++){
                    if(i!=1){
                        builder.append("\n");
                    }
                    builder.append(heighlights[i]);
                }
                String heighlightParam = builder.toString();
                YAMLFactory yamlFactory = new YAMLFactory();
                YAMLParser yamlParser = null;
                try {
                yamlParser = yamlFactory.createParser(heighlightParam.toCharArray());
                YamlXContentParser yamlXContentParser = new YamlXContentParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, yamlParser);
                Map<String, Object> map = yamlXContentParser.map();
                hintParams.add(map);
                } catch (IOException e) {
                    throw new SqlParseException("could not parse heighlight hint: " + e.getMessage());
                }
            }
            return new Hint(HintType.HIGHLIGHT,hintParams.toArray());
        }
        if(hintAsString.startsWith("! MINUS_SCROLL_FETCH_AND_RESULT_LIMITS")){
            Integer[] params = parseParamsAsInts(hintAsString,"! MINUS_SCROLL_FETCH_AND_RESULT_LIMITS");
            if( params.length>3){
                throw new SqlParseException("MINUS_FETCH_AND_RESULT_LIMITS should have 3 int params (maxFromFirst,maxFromSecond,hitsPerScrollShard)");
            }
            Integer[] paramsWithDefaults = new Integer[3];
            int defaultMaxFetchFromTable = 100000;
            int defaultFetchOnScroll = 1000;
            paramsWithDefaults[0] = defaultMaxFetchFromTable;
            paramsWithDefaults[1] = defaultMaxFetchFromTable;
            paramsWithDefaults[2] = defaultFetchOnScroll;
            for(int i=0;i<params.length;i++){
                paramsWithDefaults[i]=params[i];
            }

            return new Hint(HintType.MINUS_FETCH_AND_RESULT_LIMITS, paramsWithDefaults);
        }
        if(hintAsString.startsWith("! MINUS_USE_TERMS_OPTIMIZATION")){
            String[] param = getParamsFromHint(hintAsString,"! MINUS_USE_TERMS_OPTIMIZATION");
            boolean shouldLowerStringOnTerms = false;
            if(param!=null ){
                if(param.length!=1) {
                    throw new SqlParseException("MINUS_USE_TERMS_OPTIMIZATION should have none or one boolean param: false/true ");
                }
                try {
                    shouldLowerStringOnTerms = Boolean.parseBoolean(param[0].toLowerCase());
                }
                catch (Exception e){
                    throw new SqlParseException("MINUS_USE_TERMS_OPTIMIZATION should have none or one boolean param: false/true , got:" + param[0]);
                }
            }
            return new Hint(HintType.MINUS_USE_TERMS_OPTIMIZATION, new Object[]{shouldLowerStringOnTerms});
        }
        if (hintAsString.startsWith("! COLLAPSE")) {
            String collapse = getParamFromHint(hintAsString, "! COLLAPSE");
            return new Hint(HintType.COLLAPSE, new String[]{collapse});
        }
        if (hintAsString.startsWith("! POST_FILTER")) {
            String postFilter = getParamFromHint(hintAsString, "! POST_FILTER");
            return new Hint(HintType.POST_FILTER, new String[]{postFilter});
        }
        if (hintAsString.startsWith("! STATS")) {
            String[] statsGroups = getParamsFromHint(hintAsString, "! STATS");
            return new Hint(HintType.STATS, statsGroups);
        }
        if (hintAsString.startsWith("! CONFLICTS")) {
            String conflictsParam = getParamFromHint(hintAsString, "! CONFLICTS");
            return new Hint(HintType.CONFLICTS, new String[] { conflictsParam });
        }
        if (hintAsString.startsWith("! PREFERENCE")) {
            String preferenceParam = getParamFromHint(hintAsString, "! PREFERENCE");
            return new Hint(HintType.PREFERENCE, new String[]{preferenceParam});
        }
        if (hintAsString.startsWith("! TRACK_TOTAL_HITS")) {
            String trackTotalTitsParam = getParamFromHint(hintAsString, "! TRACK_TOTAL_HITS");
            return new Hint(HintType.TRACK_TOTAL_HITS, new String[]{trackTotalTitsParam});
        }
        if (hintAsString.startsWith("! TIMEOUT")) {
            String timeoutParam = getParamFromHint(hintAsString, "! TIMEOUT");
            return new Hint(HintType.TIMEOUT, new String[]{timeoutParam});
        }
        if (hintAsString.startsWith("! INDICES_OPTIONS")) {
            String indicesOptions = getParamFromHint(hintAsString, "! INDICES_OPTIONS");
            if (!indicesOptions.startsWith("{")) {
                indicesOptions = "{" + indicesOptions;
            }
            if (!indicesOptions.endsWith("}")) {
                indicesOptions = indicesOptions + "}";
            }
            return new Hint(HintType.INDICES_OPTIONS, new Object[]{indicesOptions});
        }
        if (hintAsString.startsWith("! MIN_SCORE")) {
            String minScoreParam = getParamFromHint(hintAsString, "! MIN_SCORE");
            return new Hint(HintType.MIN_SCORE, new String[]{minScoreParam});
        }
        if (hintAsString.startsWith("! SEARCH_AFTER")) {
            String[] searchAfterParams = getParamsFromHint(hintAsString, "! SEARCH_AFTER");
            return new Hint(HintType.SEARCH_AFTER, searchAfterParams);
        }
        if (hintAsString.startsWith("! RUNTIME_MAPPINGS")) {
            String runtimeMappings = getParamFromHint(hintAsString, "! RUNTIME_MAPPINGS");
            return new Hint(HintType.RUNTIME_MAPPINGS, new String[]{runtimeMappings});
        }

        return null;
    }

    private static String getParamFromHint(String hint, String prefix) {
        if (!hint.contains("(")) return null;
        return hint.replace(prefix, "").replaceAll("^\\s*\\(\\s*", "").replaceAll("\\s*\\,\\s*", ",").replaceAll("\\s*\\)\\s*$", "");
    }
    private static String[] getParamsFromHint(String hint, String prefix) {
        String param = getParamFromHint(hint, prefix);
        return param != null ? param.split(",") : null;
    }
    private static Integer[] parseParamsAsInts(String hintAsString,String startWith) {
        String[] number = getParamsFromHint(hintAsString,startWith);
        if(number == null){
            return new Integer[0];
        }
        //todo: check if numbers etc..
        Integer[] params = new Integer[number.length];
        for (int i = 0; i < params.length; i++) {
            params[i] = Integer.parseInt(number[i]);
        }
        return params;
    }


}
