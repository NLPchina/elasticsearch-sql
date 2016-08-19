package org.nlpcn.es4sql.sql;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.elasticsearch.common.collect.Tuple;
import org.nlpcn.es4sql.domain.KVValue;

import java.util.List;
import java.util.Random;
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


    public static Tuple<String, String> function(String methodName, List<KVValue> paramers, String name) {
        Tuple<String, String> functionStr = null;
        switch (methodName) {
            case "split":
                if (paramers.size() == 3) {
                    functionStr = split(paramers.get(0).value.toString(),
                            paramers.get(1).value.toString(),
                            Integer.parseInt(paramers.get(2).value.toString()), name);
                } else {
                    functionStr = split(paramers.get(0).value.toString(),
                            paramers.get(1).value.toString(),
                            name);
                }

                break;

            case "concat_ws":
                List<String> result = Lists.newArrayList();
                for (int i = 1; i < paramers.size(); i++) {
                    result.add(paramers.get(i).toString());
                }
                functionStr = concat_ws(paramers.get(0).value.toString(), result, name);

                break;

            case "floor":
                functionStr = floor(paramers.get(0).value.toString(), name);
                break;

            case "round":
                functionStr = round(paramers.get(0).value.toString(), name);
                break;
            case "log":
                functionStr = log(paramers.get(0).value.toString(), name);
                break;

            case "log10":
                functionStr = log10(paramers.get(0).value.toString(), name);
                break;

            case "sqrt":
                functionStr = sqrt(paramers.get(0).value.toString(), name);
                break;

            case "substring":
                functionStr = substring(paramers.get(0).value.toString(),
                        Integer.parseInt(paramers.get(1).value.toString()),
                        Integer.parseInt(paramers.get(2).value.toString())
                        , name);
                break;
            case "trim":
                functionStr = trim(paramers.get(0).value.toString(), name);
                break;

            default:

        }
        return functionStr;
    }


    public static Tuple<String, String> concat_ws(String split, List<String> columns, String name) {
        List<String> result = Lists.newArrayList();
        for (String strColumn : columns) {
            result.add("doc['" + strColumn + "'].value");

        }
        return new Tuple(name, "def value =" + Joiner.on("+'" + split + "'+").join(result) + ";value");
    }


    //split(Column str, java.lang.String pattern)
    public static Tuple<String, String> split(String strColumn, String pattern, int index, String valueName) {
        String name = "split_" + new Random().nextInt(1000000);
        if (valueName == null) {
            return new Tuple(name, "def " + name + " = doc['" + strColumn + "'].value.split('" + pattern + "')[" + index + "]");
        } else {
            return new Tuple(name, strColumn + "; def " + name + " = " + valueName + ".split('" + pattern + "')[" + index + "]");
        }

    }


    public static Tuple<String, String> log(String strColumn, String valueName) {

        return mathSingleValueTemplate("log", strColumn, valueName);

    }

    public static Tuple<String, String> log10(String strColumn, String valueName) {

        return mathSingleValueTemplate("log10", strColumn, valueName);

    }

    public static Tuple<String, String> sqrt(String strColumn, String valueName) {

        return mathSingleValueTemplate("sqrt", strColumn, valueName);

    }

    public static Tuple<String, String> round(String strColumn, String valueName) {

        return mathSingleValueTemplate("round", strColumn, valueName);

    }

    public static Tuple<String, String> trim(String strColumn, String valueName) {

        return strSingleValueTemplate("trim", strColumn, valueName);

    }

    public static Tuple<String, String> mathSingleValueTemplate(String methodName, String strColumn, String valueName) {
        String name = methodName + "_" + new Random().nextInt(1000000);
        if (valueName == null) {
            return new Tuple(name, "def " + name + " = " + methodName + "(doc['" + strColumn + "'].value)");
        } else {
            return new Tuple(name, strColumn + ";def " + name + " = " + methodName + "(" + valueName + ")");
        }

    }

    public static Tuple<String, String> strSingleValueTemplate(String methodName, String strColumn, String valueName) {
        String name = methodName + "_" + new Random().nextInt();
        if (valueName == null) {
            return new Tuple(name, "def " + name + " = doc['" + strColumn + "'].value." + methodName + "()");
        } else {
            return new Tuple(name, strColumn + "; def " + name + " = " + valueName + "." + methodName + "()");
        }

    }

    public static Tuple<String, String> floor(String strColumn, String valueName) {

        return mathSingleValueTemplate("floor", strColumn, valueName);

    }


    //substring(Column str, int pos, int len)
    public static Tuple<String, String> substring(String strColumn, int pos, int len, String valueName) {
        String name = "substring_" + new Random().nextInt(1000000);
        if (valueName == null) {
            return new Tuple(name, "def " + name + " = doc['" + strColumn + "'].value.substring(" + pos + "," + len + ")");
        } else {
            return new Tuple(name, strColumn + ";def " + name + " = " + valueName + ".substring(" + pos + "," + len + ")");
        }

    }

    //split(Column str, java.lang.String pattern)
    public static Tuple<String, String> split(String strColumn, String pattern, String valueName) {
        String name = "split_" + new Random().nextInt();
        if (valueName == null) {
            return new Tuple(name, "def " + name + " = doc['" + strColumn + "'].value.split('" + pattern + "')");
        } else {
            return new Tuple(name, strColumn + "; def " + name + " = " + valueName + ".split('" + pattern + "')");
        }

    }


}
