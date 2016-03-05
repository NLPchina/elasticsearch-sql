package org.nlpcn.es4sql.domain.hints;

import org.elasticsearch.common.jackson.dataformat.yaml.YAMLFactory;
import org.elasticsearch.common.jackson.dataformat.yaml.YAMLParser;
import org.elasticsearch.common.xcontent.yaml.YamlXContentParser;
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
        if(hintAsString.startsWith("! USE_SCROLL")){
            String[] scrollParams = getParamsFromHint(hintAsString,"! USE_SCROLL");
            int docsPerShardFetch = 50;
            int timeout = 60000;
            if(scrollParams != null && scrollParams.length ==2) {
                docsPerShardFetch = Integer.parseInt(scrollParams[0]);
                timeout = Integer.parseInt(scrollParams[1]);
            }
            return new Hint(HintType.USE_SCROLL, new Object[]{docsPerShardFetch,timeout});
        }
        if(hintAsString.startsWith("! IGNORE_UNAVAILABLE")){
            return new Hint(HintType.IGNORE_UNAVAILABLE,null);
        }
        if(hintAsString.startsWith("! DOCS_WITH_AGGREGATION")) {
            String[] number = getParamsFromHint(hintAsString,"! DOCS_WITH_AGGREGATION");
            //todo: check if numbers etc..
            int docsWithAggregation = Integer.parseInt(number[0]);
            return new Hint(HintType.DOCS_WITH_AGGREGATION,new Object[]{docsWithAggregation});
        }
        if(hintAsString.startsWith("! ROUTINGS")) {
            String[] routings = getParamsFromHint(hintAsString,"! ROUTINGS");
            return new Hint(HintType.ROUTINGS,routings);
        }
        if(hintAsString.startsWith("! HIGHLIGHT")) {
            String[] heighlights = getParamsFromHint(hintAsString,"! HIGHLIGHT");
            ArrayList hintParams = new ArrayList();
            hintParams.add(heighlights[0]);
            if(heighlights.length == 2){
                String heighlightParam = heighlights[1];
                heighlightParam = heighlightParam.replaceAll(" , ", "\n");
                YAMLFactory yamlFactory = new YAMLFactory();
                YAMLParser parser1 = null;
                try {
                parser1 = yamlFactory.createParser(heighlightParam.toCharArray());
                YamlXContentParser yamlXContentParser = new YamlXContentParser(parser1);
                Map<String, Object> map = yamlXContentParser.map();
                hintParams.add(map);
                } catch (IOException e) {
                    throw new SqlParseException("could not parse heighlight hint: " + e.getMessage());
                }
            }
            return new Hint(HintType.HIGHLIGHT,hintParams.toArray());
        }

        return null;
    }


    private static String[] getParamsFromHint(String hint, String prefix) {
        if(!hint.contains("(")) return null;
        String onlyParams = hint.replace(prefix, "").replaceAll("\\s*\\(\\s*","").replaceAll("\\s*\\,\\s*", ",").replaceAll("\\s*\\)\\s*", "");
        return onlyParams.split(",");
    }


}
