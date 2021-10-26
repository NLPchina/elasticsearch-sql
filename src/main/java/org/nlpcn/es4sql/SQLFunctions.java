package org.nlpcn.es4sql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntervalExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumericLiteralExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.util.StringUtils;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.elasticsearch.core.Tuple;
import org.nlpcn.es4sql.domain.KVValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by allwefantasy on 8/19/16.
 */
public class SQLFunctions {

    //Groovy Built In Functions
    public final static Set<String> buildInFunctions = Sets.newHashSet(
            "exp", "log", "log2", "log10", "log10", "sqrt", "cbrt", "ceil", "floor", "rint", "pow", "round",
            "random", "abs", //nummber operator
            "split", "concat_ws", "substring", "trim",//string operator
            "add", "multiply", "divide", "subtract", "modulus",//binary operator
            "field", "date_format", "if",//if判断目前支持多个二元操作符
            "max_bw", "min_bw", //added by xzb 取两个数的最大/小值
            "coalesce", //added by xzb  取两个值中间有值的那个
            "case_new",//added by xzb 支持多个判断条件
            //支持正则表达式抽取原字段后赋给新字段,注意必须指定一个group。如 parse(hobby,(?<type>\S+)球, defaultValue)
            "parse",//此函数需要在elasticsearch.yml中设置 script.painless.regex.enabled : true
            "now", "date", "date_add", "from_unixtime"
    );
    //added by xzb 增加二元操作运算符
        public static Set<String> binaryOperators = Sets.newHashSet("=" ,"!=", ">", ">=", "<", "<=");

    //modified by xzb 增加 binaryOperatorName，即 if、case条件中的判断
    public static Tuple<String, String> function(String methodName, List<KVValue> paramers, String name, boolean returnValue , String  binaryOperatorName, List<String> binaryOperatorNames) throws Exception {
        //added by xzb ,默认二元操作符为 ==
        if (binaryOperatorName == null || binaryOperatorName.equals("=")) {
            binaryOperatorName = " == ";
        }

        Tuple<String, String> functionStr = null;
        switch (methodName.toLowerCase()) {
            case "if":
                String nameIF = "";
                String caseString = "";
                if(paramers.get(0).value instanceof SQLInListExpr){
                    nameIF += methodName+"("+((SQLInListExpr) paramers.get(0).value).getExpr()+" in (";
                    String left = "doc['"+((SQLInListExpr) paramers.get(0).value).getExpr().toString()+"'].value";
                    List<SQLExpr> targetList = ((SQLInListExpr) paramers.get(0).value).getTargetList();
                    for(SQLExpr a:targetList){
                        caseString += left + " == '" + a.toString() + "' ||";
                        nameIF += a.toString()+",";
                    }
                    caseString = caseString.substring(0,caseString.length()-2);
                    nameIF = nameIF.substring(0,nameIF.length()-1)+"),";
                }else{
                    String key  =paramers.get(0).key;
                    String left = "doc['"+key+"'].value";
                    String value = paramers.get(0).value.toString();
                   //xzb  支持更多的表达式，如 > 、<、>=、<=、!= 等
                    caseString += left + binaryOperatorName + value;
                    nameIF = methodName+"("+ key + binaryOperatorName + value +",";
                }
                nameIF += paramers.get(1).value+","+paramers.get(2).value+")";
                functionStr = new Tuple<>(nameIF,"if(("+caseString+")){"+paramers.get(1).value+"} else {"+paramers.get(2).value+"}");
                break;
            case "split":
                if (paramers.size() == 3) {
                    functionStr = split(Util.expr2Object((SQLExpr) paramers.get(0).value).toString(),
                            Util.expr2Object((SQLExpr) paramers.get(1).value).toString(),
                            Integer.parseInt(Util.expr2Object((SQLExpr) paramers.get(2).value).toString()), name);
                } else {
                    functionStr = split(paramers.get(0).value.toString(),
                            paramers.get(1).value.toString(),
                            name);
                }

                break;

            case "concat_ws":
                List<SQLExpr> result = Lists.newArrayList();
                for (int i = 1; i < paramers.size(); i++) {
                    result.add((SQLExpr) paramers.get(i).value);
                }
                functionStr = concat_ws(paramers.get(0).value.toString(), result, name);

                break;


            case "date_format":
                functionStr = date_format(
                        Util.expr2Object((SQLExpr) paramers.get(0).value).toString(),
                        Util.expr2Object((SQLExpr) paramers.get(1).value).toString(),
                        2 < paramers.size() ? Util.expr2Object((SQLExpr) paramers.get(2).value).toString() : null,
                        name);
                break;
            case "from_unixtime":
                functionStr = from_unixtime(
                        Util.expr2Object((SQLExpr) paramers.get(0).value).toString(),
                        1 < paramers.size() ? Util.expr2Object((SQLExpr) paramers.get(1).value).toString() : null,
                        2 < paramers.size() ? Util.expr2Object((SQLExpr) paramers.get(2).value).toString() : null,
                        name);
                break;

            case "abs":
            case "round":
            case "max_bw":
            case "min_bw":
            case "coalesce":
            case "parse":
            case "case_new":
            case "floor":
                //zhongshu-comment es的round()默认是保留到个位，这里给round()函数加上精确到小数点后第几位的功能
                //modify by xzb 增加两个函数 min_bw 和 max_bw
                if (paramers.size() >= 2) {//coalesce函数的参数可以是2个以上
                    if (methodName.equals("round")){
                        int decimalPrecision = Integer.parseInt(paramers.get(1).value.toString());
                        functionStr = mathRoundTemplate("Math."+methodName,methodName,Util.expr2Object((SQLExpr) paramers.get(0).value).toString(), name, decimalPrecision);
                        break;
                    } else if (methodName.equals("max_bw")) {
                        functionStr = mathBetweenTemplate("Math.max", methodName, paramers, name);
                        break;
                    }  else if (methodName.equals("min_bw")) {
                        functionStr = mathBetweenTemplate("Math.min", methodName, paramers, name);
                        break;
                    } else if (methodName.equals("coalesce")) {
                        functionStr = coalesceTemplate(methodName, paramers);
                        break;
                    }else if (methodName.equals("case_new")) {
                        functionStr = caseNewTemplate(methodName, paramers, binaryOperatorNames);
                        break;
                    }else if (methodName.equals("parse")) {
                        functionStr = parseTemplate(methodName, paramers);
                        break;
                    }
                }
            case "ceil":
            case "cbrt":
            case "rint":
            case "exp":
            case "sqrt":
                functionStr = mathSingleValueTemplate("Math."+methodName,methodName,Util.expr2Object((SQLExpr) paramers.get(0).value).toString(), name);
                break;

            case "pow":
                functionStr = mathDoubleValueTemplate("Math."+methodName, methodName, Util.expr2Object((SQLExpr) paramers.get(0).value).toString(), Util.expr2Object((SQLExpr) paramers.get(1).value).toString(), name);
                break;

            case "substring":
                functionStr = substring(Util.expr2Object((SQLExpr) paramers.get(0).value).toString(),
                        Integer.parseInt(Util.expr2Object((SQLExpr) paramers.get(1).value).toString()),
                        Integer.parseInt(Util.expr2Object((SQLExpr) paramers.get(2).value).toString())
                        , name);
                break;
            case "trim":
                functionStr = trim(Util.expr2Object((SQLExpr) paramers.get(0).value).toString(), name);
                break;

            case "add":
                functionStr = add((SQLExpr) paramers.get(0).value, (SQLExpr) paramers.get(1).value);
                break;

            case "subtract":
                functionStr = subtract((SQLExpr) paramers.get(0).value, (SQLExpr) paramers.get(1).value);
                break;
            case "divide":
                functionStr = divide((SQLExpr) paramers.get(0).value, (SQLExpr) paramers.get(1).value);
                break;

            case "multiply":
                functionStr = multiply((SQLExpr) paramers.get(0).value, (SQLExpr) paramers.get(1).value);
                break;
            case "modulus":
                functionStr = modulus((SQLExpr) paramers.get(0).value, (SQLExpr) paramers.get(1).value);
                break;

            case "field":
                functionStr = field(Util.expr2Object((SQLExpr) paramers.get(0).value).toString());
                break;

            case "log2":
                functionStr = log(SQLUtils.toSQLExpr("2"), (SQLExpr) paramers.get(0).value, name);
                break;
            case "log10":
                functionStr = log(SQLUtils.toSQLExpr("10"), (SQLExpr) paramers.get(0).value, name);
                break;
            case "log":
                List<SQLExpr> logs = Lists.newArrayList();
                for (int i = 0; i < paramers.size(); i++) {
                    logs.add((SQLExpr) paramers.get(0).value);
                }
                if (logs.size() > 1) {
                    functionStr = log(logs.get(0), logs.get(1), name);
                } else {
                    functionStr = log(SQLUtils.toSQLExpr("Math.E"), logs.get(0), name);
                }
                break;

            case "now":
                functionStr = now();
                break;
            case "date":
                functionStr = date(Util.expr2Object((SQLExpr) paramers.get(0).value).toString(), name);
                break;
            case "date_add":
                functionStr = date_add(
                        Util.expr2Object((SQLExpr) paramers.get(0).value).toString(),
                        (SQLIntervalExpr) paramers.get(1).value,
                        name);
                break;
            default:

        }

        //added by xzb 以下几种情况的脚本，script中均不需要return语句
        if(returnValue && !methodName.equalsIgnoreCase("if") &&
                !methodName.equalsIgnoreCase("coalesce") &&
                !methodName.equalsIgnoreCase("parse") &&
                !methodName.equalsIgnoreCase("case_new") &&
                buildInFunctions.contains(methodName)){
            String generatedFieldName = functionStr.v1();
            String returnCommand = ";return " + generatedFieldName +";" ;
            String newScript = functionStr.v2() + returnCommand;
            functionStr = new Tuple<>(generatedFieldName, newScript);
        }
        return functionStr;
    }

    public static String random() {
        return Math.abs(ThreadLocalRandom.current().nextInt()) + "";
    }

    private static Tuple<String, String> concat_ws(String split, List<SQLExpr> columns, String valueName) {
        String name = "concat_ws_" + random();
        List<String> result = Lists.newArrayList();

        for (SQLExpr column : columns) {
            String strColumn = Util.expr2Object(column).toString();
            if (strColumn.startsWith("def ")) {
                result.add(strColumn);
            } else if (isProperty(column)) {
                result.add("doc['" + strColumn + "'].value");
            } else {
                result.add("'" + strColumn + "'");
            }

        }
        return new Tuple<>(name, "def " + name + " =" + Joiner.on("+ " + split + " +").join(result));

    }


    //split(Column str, java.lang.String pattern)
    public static Tuple<String, String> split(String strColumn, String pattern, int index, String valueName) {
        String name = "split_" + random();
        String script = "";
        if (valueName == null) {
            script = "def " + name + " = doc['" + strColumn + "'].value.split('" + pattern + "')[" + index + "]";

        } else {
            script = "; def " + name + " = " + valueName + ".split('" + pattern + "')[" + index + "]";
        }
        return new Tuple<>(name, script);
    }

    private static Tuple<String, String> date_format(String strColumn, String pattern, String zoneId, String valueName) {
        String name = "date_format_" + random();
        if (valueName == null) {
            return new Tuple<>(name, "def " + name + " = DateTimeFormatter.ofPattern('" + pattern + "').withZone(" +
                    (zoneId != null ? "ZoneId.of('" + zoneId + "')" : "ZoneId.systemDefault()") +
                    ").format(Instant.ofEpochMilli(doc['" + strColumn + "'].value.getMillis()))");
        } else {
            return new Tuple<>(name, strColumn + "; def " + name + " = new SimpleDateFormat('" + pattern + "').format(new Date(" + valueName + " - 8*1000*60*60))");
        }

    }

    private static Tuple<String, String> from_unixtime(String strColumn, String pattern, String zoneId, String valueName) {
        String name = "from_unixtime_" + random();

        if (Objects.isNull(pattern)) {
            pattern = "yyyy-MM-dd HH:mm:ss";
        }

        zoneId = Objects.isNull(zoneId) ? "ZoneId.systemDefault()" : "ZoneId.of('" + zoneId + "')";

        if (valueName == null) {
            return new Tuple<>(name, "def " + name + " = DateTimeFormatter.ofPattern('" + pattern + "').withZone(" +
                    zoneId +
                    ").format(Instant.ofEpochSecond(doc['" + strColumn + "'].value))");
        } else {
            return new Tuple<>(name, strColumn + "; def " + name + " = DateTimeFormatter.ofPattern('" + pattern + "').withZone(" +
                    zoneId +
                    ").format(Instant.ofEpochSecond(" + valueName + "))");
        }
    }

    public static Tuple<String, String> add(SQLExpr a, SQLExpr b) {
        return binaryOpertator("add", "+", a, b);
    }

    private static Tuple<String, String> modulus(SQLExpr a, SQLExpr b) {
        return binaryOpertator("modulus", "%", a, b);
    }

    public static Tuple<String, String> field(String a) {
        String name = "field_" + random();
        return new Tuple<>(name, "def " + name + " = " + "doc['" + a + "'].value");
    }

    private static Tuple<String, String> subtract(SQLExpr a, SQLExpr b) {
        return binaryOpertator("subtract", "-", a, b);
    }

    private static Tuple<String, String> multiply(SQLExpr a, SQLExpr b) {
        return binaryOpertator("multiply", "*", a, b);
    }

    private static Tuple<String, String> divide(SQLExpr a, SQLExpr b) {
        return binaryOpertator("divide", "/", a, b);
    }

    private static Tuple<String, String> binaryOpertator(String methodName, String operator, SQLExpr a, SQLExpr b) {

        String name = methodName + "_" + random();
        return new Tuple<>(name,
                scriptDeclare(a) + scriptDeclare(b) +
                        convertType(a) + convertType(b) +
                        " def " + name + " = " + extractName(a) + " " + operator + " " + extractName(b) ) ;
    }

    private static boolean isProperty(SQLExpr expr) {
        return (expr instanceof SQLIdentifierExpr || expr instanceof SQLPropertyExpr || expr instanceof SQLVariantRefExpr);
    }

    private static String scriptDeclare(SQLExpr a) {

        if (isProperty(a) || a instanceof SQLNumericLiteralExpr)
            return "";
        else return Util.expr2Object(a).toString() + ";";
    }

    private static String extractName(SQLExpr script) {
        if (isProperty(script)) return "doc['" + script + "'].value";
        String scriptStr = Util.expr2Object(script).toString();
        String[] variance = scriptStr.split(";");
        String newScript = variance[variance.length - 1];
        if (newScript.trim().startsWith("def ")) {
            //for now ,if variant is string,then change to double.
            return newScript.trim().substring(4).split("=")[0].trim();
        } else return scriptStr;
    }

    //cast(year as int)

    private static String convertType(SQLExpr script) {
        String[] variance = Util.expr2Object(script).toString().split(";");
        String newScript = variance[variance.length - 1];
        if (newScript.trim().startsWith("def ")) {
            //for now ,if variant is string,then change to double.
            String temp = newScript.trim().substring(4).split("=")[0].trim();

            return " if( " + temp + " instanceof String) " + temp + "= Double.parseDouble(" + temp.trim() + "); ";
        } else return "";


    }


    public static Tuple<String, String> log(String strColumn, String valueName) {

        return mathSingleValueTemplate("log", strColumn, valueName);

    }

    public static Tuple<String, String> log10(String strColumn, String valueName) {

        return mathSingleValueTemplate("log10", strColumn, valueName);

    }
    public static Tuple<String, String> log(SQLExpr base, SQLExpr strColumn, String valueName) {
        String name = "log_" + random();
        String result;
        if (valueName == null) {
            if (isProperty(strColumn)) {
                result = "def " + name + " = Math.log(doc['" + Util.expr2Object(strColumn).toString() + "'].value)/Math.log("+Util.expr2Object(base).toString()+")";
            } else {
                result = "def " + name + " = Math.log(" + Util.expr2Object(strColumn).toString() + ")/Math.log("+Util.expr2Object(base).toString()+")";
            }
        } else {
            result = Util.expr2Object(strColumn).toString()+";def "+name+" = Math.log("+valueName+")/Math.log("+Util.expr2Object(base).toString()+")";
        }
        return new Tuple(name, result);
    }

    public static Tuple<String, String> sqrt(String strColumn, String valueName) {

        return mathSingleValueTemplate("Math.sqrt", "sqrt",  strColumn, valueName);

    }

    public static Tuple<String, String> round(String strColumn, String valueName) {

        return mathSingleValueTemplate("Math.round","round", strColumn, valueName);

    }

    public static Tuple<String, String> trim(String strColumn, String valueName) {

        return strSingleValueTemplate("trim", strColumn, valueName);

    }

    private static Tuple<String, String> mathDoubleValueTemplate(String methodName, String fieldName, String val1, String val2, String valueName) {
        String name = fieldName + "_" + random();
        if (valueName == null) {
            return new Tuple(name, "def "+name+" = "+methodName+"(doc['"+val1+"'].value, "+val2+")");
        } else {
            return new Tuple(name, val1 + ";def "+name+" = "+methodName+"("+valueName+", "+val2+")");
        }
    }

    private static Tuple<String, String> mathSingleValueTemplate(String methodName, String strColumn, String valueName) {
        return mathSingleValueTemplate(methodName,methodName, strColumn,valueName);
    }
    private static Tuple<String, String> mathSingleValueTemplate(String methodName, String fieldName, String strColumn, String valueName) {
        String name = fieldName + "_" + random();
        if (valueName == null) {
            return new Tuple<>(name, "def " + name + " = " + methodName + "(doc['" + strColumn + "'].value)");
        } else {
            return new Tuple<>(name, strColumn + ";def " + name + " = " + methodName + "(" + valueName + ")");
        }

    }

    private static Tuple<String, String> mathRoundTemplate(String methodName, String fieldName, String strColumn, String valueName, int decimalPrecision) {

        StringBuilder sb = new StringBuilder("1");
        for (int i = 0; i < decimalPrecision; i++) {
            sb.append("0");
        }
        double num = Double.parseDouble(sb.toString());

        String name = fieldName + "_" + random();
        if (valueName == null) {
            return new Tuple<>(name, "def " + name + " = " + methodName + "((doc['" + strColumn + "'].value) * " + num + ")/" + num);
        } else {
            return new Tuple<>(name, strColumn + ";def " + name + " = " + methodName + "((" + valueName + ") * " + num + ")/" + num);
        }

    }

    //求两个值中最大值，如 def abs_775880898 = Math.max(doc['age1'].value, doc['age2'].value);return abs_775880898;
    private static Tuple<String, String> mathBetweenTemplate(String methodName, String fieldName, List<KVValue> paramer, String valueName) {
        //获取 max_bw/min_bw 函数的两个字段
        String name = fieldName + "_" + random();
        StringBuffer sb = new StringBuffer();
        sb.append("def " + name + " = " + methodName + "(");
        int i = 0;
        for (KVValue kv : paramer) {
            String field = kv.value.toString();
            if (i > 0) {
                sb.append(", ");
            }
            sb.append("doc['" + field + "'].value");
            i++;
        }
        sb.append(")");
        return new Tuple<>(name, sb.toString());
    }

   //实现coalesce(field1, field2, ...)功能，只要任意一个不为空即可
    private static Tuple<String, String> coalesceTemplate(String fieldName, List<KVValue> paramer) {
        //if((doc['age2'].value != null)){doc['age2'].value} else if((doc['age1'].value != null)){doc['age1'].value}
        String name = fieldName + "_" + random();
        StringBuffer sb = new StringBuffer();
        int i = 0;
        //sb.append("def " + name + " = ");
        for (KVValue kv : paramer) {
            String field = kv.value.toString();
            if (i > 0) {
                sb.append(" else ");
            }
            sb.append("if(doc['" + field + "'].value != null){doc['" + field + "'].value}");
            i++;
        }
        return new Tuple<>(name, sb.toString());
    }

    //实现正则表达式抽取原字段后赋给新字段,注意必须指定一个group。如 parse(hobby,(?<type>\S+)球, defaultValue)
    //"SELECT  parse(hobby, '(?<type>\\\\S+)球', 'NOT_MATCH') AS ballType, COUNT(_index) FROM bank GROUP BY ballType"
    private static Tuple<String, String> parseTemplate(String fieldName, List<KVValue> params) {
       //  def m = /(?<type>\S+)球/.matcher(doc['hobby'].value); if(m.matches()) { return m.group(1) } else { return \"no_match\" }
        String name = fieldName + "_" + random();
        StringBuffer sb = new StringBuffer();
        if (null == params || params.size()!= 3) {
            throw new IllegalArgumentException("parse 函数必须包含三个参数，第一个是原字段，第二个是带有group的正则表达式, 第三个是抽取不成功的默认值");
        }
        String srcField = params.get(0).value.toString();
        String regexStr = params.get(1).value.toString();
        //需要去除自动添加的单引号
        regexStr = regexStr.substring(1, regexStr.length() - 1);
        String defaultValue = params.get(2).value.toString();

        sb.append("def m = /" + regexStr + "/.matcher(doc['" + srcField + "'].value); if(m.matches()) { return m.group(1) } else { return " + defaultValue +" }");
        return new Tuple<>(name, sb.toString());
    }

    //实现   case_new(gender='m', '男', gender='f', '女',  default, '无') as myGender  功能
    private static Tuple<String, String> caseNewTemplate(String fieldName, List<KVValue> paramer, List<String> binaryOperatorNames) {
        if (paramer.size() % 2 != 0) {//如果参数不是偶数个，则抛异常
            throw new IllegalArgumentException("请检查参数数量，必须是偶数个！");
        }
        //1.找出所有字段及其对应的值存入到Map中，如果有default，则将其移除
        String defaultVal = null;
        List<String> fieldList = new ArrayList<>();
        List<Object> valueList = new ArrayList<>();
        List<Object> defaultList = new ArrayList<>();
        for (int i = 0; i < paramer.size(); i = i + 2) {
            String _default = paramer.get(i + 1).value.toString();
            //记录默认值
            if (paramer.get(i).value.toString().equalsIgnoreCase("default")) {
                 defaultVal = _default;
            } else {
                fieldList.add(paramer.get(i).key);
                valueList.add(paramer.get(i).value.toString());
                defaultList.add(_default);
            }
        }
        //  if((doc['gender'].value == 'm')) '男' else if((doc['gender'].value == 'f')) '女' else ''无
        String name = fieldName + "_" + random();
        StringBuffer sb = new StringBuffer();
        int i = 0;
        //sb.append("def " + name + " = ");
        for (int j = 0; j < fieldList.size(); j++) {
            String field = fieldList.get(j);
            if (i > 0) {
                sb.append(" else ");
            }
            //added by xzb 此处有问题，还需要支持除 == 外的其他二元操作符
           // sb.append("if(doc['" + field + "'].value == " + valueList.get(i) + ") { " + defaultList.get(i) + " }");
            String binaryOperatorName = binaryOperatorNames.get(j);
            if ("=".equals(binaryOperatorName)) {// SQL中只有 = 符号，但script中必须使用 ==
                binaryOperatorName = "==";
            }
            sb.append("if(doc['" + field + "'].value " + binaryOperatorName + " " + valueList.get(i) + ") { " + defaultList.get(i) + " }");
            i++;
        }
        if (!StringUtils.isEmpty(defaultVal)) {
            sb.append(" else " + defaultVal);
        }
        return new Tuple<>(name, sb.toString());
    }

    public static Tuple<String, String> strSingleValueTemplate(String methodName, String strColumn, String valueName) {
        String name = methodName + "_" + random();
        if (valueName == null) {
            return new Tuple(name, "def " + name + " = doc['" + strColumn + "'].value." + methodName + "()" );
        } else {
            return new Tuple(name, strColumn + "; def " + name + " = " + valueName + "." + methodName + "()");
        }

    }

    public static Tuple<String, String> floor(String strColumn, String valueName) {

        return mathSingleValueTemplate("Math.floor", "floor",strColumn, valueName);

    }


    //substring(Column str, int pos, int len)
    public static Tuple<String, String> substring(String strColumn, int pos, int len, String valueName) {
        String name = "substring_" + random();
        if (valueName == null) {
            return new Tuple(name, "def " + name + " = doc['" + strColumn + "'].value.substring(" + pos + "," + len + ")");
        } else {
            return new Tuple(name, strColumn + ";def " + name + " = " + valueName + ".substring(" + pos + "," + len + ")");
        }

    }

    //split(Column str, java.lang.String pattern)
    public static Tuple<String, String> split(String strColumn, String pattern, String valueName) {
        String name = "split_" + random();
        if (valueName == null) {
            return new Tuple(name, "def " + name + " = doc['" + strColumn + "'].value.split('" + pattern + "')" );
        } else {
            return new Tuple(name, strColumn + "; def " + name + " = " + valueName + ".split('" + pattern + "')");
        }

    }

    private static Tuple<String, String> now() {
        String name = "now_" + random();
        return new Tuple<>(name, "def " + name + " = " + "Instant.ofEpochMilli(System.currentTimeMillis()).atZone(ZoneId.systemDefault())");
    }

    private static Tuple<String, String> date(String strColumn, String valueName) {
        String name = "date_" + random();
        if (valueName == null) {
            return new Tuple<>(name, "def " + name + " = doc['" + strColumn + "'].value.truncatedTo(ChronoUnit.DAYS)");
        } else {
            return new Tuple<>(name, strColumn + "; def " + name + " = " + valueName + ".truncatedTo(ChronoUnit.DAYS)");
        }
    }

    private static Tuple<String, String> date_add(String strColumn, SQLIntervalExpr intervalExpr, String valueName) {
        String unit;
        switch (intervalExpr.getUnit()) {
            case MICROSECOND:
                unit = "ChronoUnit.MICROS";
                break;
            case SECOND:
                unit = "ChronoUnit.SECONDS";
                break;
            case MINUTE:
                unit = "ChronoUnit.MINUTES";
                break;
            case HOUR:
                unit = "ChronoUnit.HOURS";
                break;
            case DAY:
                unit = "ChronoUnit.DAYS";
                break;
            case WEEK:
                unit = "ChronoUnit.WEEKS";
                break;
            case MONTH:
                unit = "ChronoUnit.MONTHS";
                break;
            case YEAR:
                unit = "ChronoUnit.YEARS";
                break;
            default:
                throw new IllegalArgumentException("not supported unit: " + intervalExpr.getUnit());
        }
        Object amountToAdd = Util.expr2Object(intervalExpr.getValue());

        String name = "date_add_" + random();
        if (valueName == null) {
            return new Tuple<>(name, "def " + name + " = doc['" + strColumn + "'].value.plus(" + amountToAdd + "," + unit + ")");
        } else {
            return new Tuple<>(name, strColumn + "; def " + name + " = " + valueName + ".plus(" + amountToAdd + "," + unit + ")");
        }
    }
}
