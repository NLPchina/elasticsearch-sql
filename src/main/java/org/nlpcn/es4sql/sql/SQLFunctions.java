package org.nlpcn.es4sql.sql;

import com.google.common.collect.Sets;
import org.nlpcn.es4sql.domain.KVValue;

import java.util.List;
import java.util.Set;

/**
 * Created by allwefantasy on 8/19/16.
 */
public class SQLFunctions {

    //Groovy Built In Functions
    public final static Set<String> buildInFunctions = Sets.newHashSet(
            "exp", "log", "log10", "sqrt", "cbrt", "ceil", "floor", "rint", "pow", "round",
            "random", "abs"
    );


    public static String function(String methodName, List<KVValue> paramers) {
        String functionStr = null;
        switch (methodName) {
            case "split":
                if (paramers.size() == 3) {
                    functionStr = split(paramers.get(0).value.toString(),
                            paramers.get(1).value.toString(),
                            Integer.parseInt(paramers.get(2).value.toString()));
                } else {
                    functionStr = split(paramers.get(0).value.toString(),
                            paramers.get(1).value.toString()
                    );
                }

                break;

            default:

        }
        return functionStr;
    }

    //split(Column str, java.lang.String pattern)
    public static String split(String strColumn, String pattern, int index) {
        return "doc['" + strColumn + "'].value.split('" + pattern + "')[" + index + "]";
    }

    //split(Column str, java.lang.String pattern)
    public static String split(String strColumn, String pattern) {
        return "doc['" + strColumn + "'].value.split('" + pattern + "')";
    }

}
