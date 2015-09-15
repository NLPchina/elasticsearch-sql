package org.nlpcn.es4sql.domain.hints;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Eliran on 5/9/2015.
 */
public class HintFactory {

    public static Hint getHintFromString(String hintAsString){
        if(hintAsString.startsWith("! USE_NESTED_LOOPS") || hintAsString.startsWith("! USE_NL")){
            return new Hint(HintType.USE_NESTED_LOOPS,null);
        }
        if(hintAsString.equals("! HASH_WITH_TERMS_FILTER"))
            return new Hint(HintType.HASH_WITH_TERMS_FILTER,null);
        if(hintAsString.startsWith("! JOIN_TABLES_LIMIT")){
            String[] numbers =  getParamsFromHint(hintAsString, "! JOIN_TABLES_LIMIT");
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
        return null;
    }


    private static String[] getParamsFromHint(String hint, String prefix) {
        String onlyParams = hint.replace(prefix, "").replaceAll("\\s*\\(\\s*","").replaceAll("\\s*\\,\\s*", ",").replaceAll("\\s*\\)\\s*", "");
        return onlyParams.split(",");
    }


}
