package org.nlpcn.es4sql;


import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugin.nlpcn.QueryActionElasticExecutor;
import org.elasticsearch.plugin.nlpcn.executors.CSVResult;
import org.elasticsearch.plugin.nlpcn.executors.CSVResultsExtractor;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.nlpcn.es4sql.domain.Condition;
import org.nlpcn.es4sql.domain.Select;
import org.nlpcn.es4sql.domain.Where;
import org.nlpcn.es4sql.parse.ElasticSqlExprParser;
import org.nlpcn.es4sql.parse.ScriptFilter;
import org.nlpcn.es4sql.parse.SqlParser;
import org.nlpcn.es4sql.query.QueryAction;

import java.net.UnknownHostException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_PEOPLE;

/**
 * Created by allwefantasy on 8/25/16.
 */
public class SQLFunctionsTest {

    private static SqlParser parser;

    @BeforeClass
    public static void init() {
        parser = new SqlParser();
    }

    @Test
    public void functionFieldAliasAndGroupByAlias() throws Exception {
        String query = "SELECT " +
                "floor(substring(address,0,3)*20) as key," +
                "sum(age) cvalue FROM " + TEST_INDEX_ACCOUNT + "/account where address is not null " +
                "group by key order by cvalue desc limit 10  ";
        SearchDao searchDao = MainTestSuite.getSearchDao() != null ? MainTestSuite.getSearchDao() : getSearchDao();
        System.out.println(searchDao.explain(query).explain().explain());

        CSVResult csvResult = getCsvResult(false, query);
        List<String> headers = csvResult.getHeaders();
        List<String> content = csvResult.getLines();
        Assert.assertEquals(2, headers.size());
        Assert.assertTrue(headers.contains("key"));
        Assert.assertTrue(headers.contains("cvalue"));
    }

    @Test
    public void functionAlias() throws Exception {
        //here is a bug,if only script fields are included,then all fields will return; fix later
        String query = "SELECT " +
                "substring(address,0,3) as key,address from " +
                TEST_INDEX_ACCOUNT + "/account where address is not null " +
                "order by address desc limit 10  ";

        CSVResult csvResult = getCsvResult(false, query);
        List<String> headers = csvResult.getHeaders();
        List<String> content = csvResult.getLines();
        Assert.assertTrue(headers.contains("key"));
        Assert.assertTrue(content.contains("863,863 Wythe Place"));
    }

    @Test
    public void normalFieldAlias() throws Exception {

        //here is a bug,csv field with spa
        String query = "SELECT " +
                "address as key,age from " +
                TEST_INDEX_ACCOUNT + "/account where address is not null " +
                "limit 10  ";

        CSVResult csvResult = getCsvResult(false, query);
        List<String> headers = csvResult.getHeaders();
        Assert.assertTrue(headers.contains("key"));
    }


    @Test
    public void groupByFieldAlias() throws Exception {

        //here is a bug,csv field with spa
        String query = "SELECT " +
                "age as key,sum(age) from " +
                TEST_INDEX_ACCOUNT + "/account where address is not null " +
                " group by key limit 10  ";

        CSVResult csvResult = getCsvResult(false, query);
        List<String> headers = csvResult.getHeaders();
        List<String> contents = csvResult.getLines();
        Assert.assertTrue(headers.contains("key"));
        String[] splits = contents.get(0).split(",");
        Assert.assertTrue(Integer.parseInt(splits[0]) <= Double.parseDouble(splits[1]));
    }

    @Test
    public void concat_ws_field_and_string() throws Exception {

        //here is a bug,csv field with spa
        String query = "SELECT " +
                " concat_ws('-',age,'-'),address from " +
                TEST_INDEX_ACCOUNT + "/account " +
                " limit 10  ";

        CSVResult csvResult = getCsvResult(false, query);
        List<String> headers = csvResult.getHeaders();
        List<String> contents = csvResult.getLines();
        String[] splits = contents.get(0).split(",");
        Assert.assertTrue(splits[0].endsWith("--") || splits[1].endsWith("--"));
    }

    @Test
    public void test() throws Exception {

        String query = "select sum(case \n" +
                "             when traffic=0 then 100 \n" +
                "             when traffic=1 then 1000 \n" +
                "             else 10000 \n" +
                "       end) as tf,date_format(5minute,'yyyyMMddHHmm') as nt  from traffic_statistics_v4_m200106 where business_line='2'   group by nt order by tf asc limit 10";

        SearchDao searchDao = MainTestSuite.getSearchDao() != null ? MainTestSuite.getSearchDao() : getSearchDao();
        System.out.println(searchDao.explain(query).explain().explain());
    }

// todo: change when split is back on language
//    @Test
//    public void whereConditionLeftFunctionRightVariableEqualTest() throws Exception {
//
//        String query = "SELECT " +
//                " * from " +
//                TestsConstants.TEST_INDEX + "/account " +
//                " where split(address,' ')[0]='806' limit 1000  ";
//
//        CSVResult csvResult = getCsvResult(false, query);
//        List<String> contents = csvResult.getLines();
//        Assert.assertTrue(contents.size() == 4);
//    }
//
//    @Test
//    public void whereConditionLeftFunctionRightVariableGreatTest() throws Exception {
//
//        String query = "SELECT " +
//                " * from " +
//                TestsConstants.TEST_INDEX + "/account " +
//                " where floor(split(address,' ')[0]+0) > 805 limit 1000  ";
//
//        SearchDao searchDao = MainTestSuite.getSearchDao() != null ? MainTestSuite.getSearchDao() : getSearchDao();
//        System.out.println(searchDao.explain(query).explain().explain());
//
//        CSVResult csvResult = getCsvResult(false, query);
//        List<String> contents = csvResult.getLines();
//        Assert.assertTrue(contents.size() == 223);
//    }

    @Test
    public void whereConditionLeftFunctionRightPropertyGreatTest() throws Exception {

        String query = "SELECT " +
                " * from " +
                TEST_INDEX_ACCOUNT + "/account " +
                " where floor(split(address,' ')[0]+0) > b limit 1000  ";

        Select select = parser.parseSelect((SQLQueryExpr) queryToExpr(query));
        Where where = select.getWhere();
        Assert.assertTrue((where.getWheres().size() == 1));
        Assert.assertTrue(((Condition) (where.getWheres().get(0))).getValue() instanceof ScriptFilter);
        ScriptFilter scriptFilter = (ScriptFilter) (((Condition) (where.getWheres().get(0))).getValue());

        Assert.assertTrue(scriptFilter.getScript().contains("doc['address'].value.split(' ')[0]"));
        Pattern pattern = Pattern.compile("\\(\\(Comparable\\)floor_\\d+\\).compareTo\\(doc\\['b'\\].value\\) > 0");
        Matcher matcher = pattern.matcher(scriptFilter.getScript());
        Assert.assertTrue(matcher.find());

    }

    private SQLExpr queryToExpr(String query) {
        return new ElasticSqlExprParser(query).expr();
    }

    @Test
    public void whereConditionLeftFunctionRightFunctionEqualTest() throws Exception {

        String query = "SELECT " +
                " * from " +
                TEST_INDEX_ACCOUNT + "/account " +
                " where floor(split(address,' ')[0]+0) = floor(split(address,' ')[0]+0) limit 1000  ";

        Select select = parser.parseSelect((SQLQueryExpr) queryToExpr(query));
        Where where = select.getWhere();
        Assert.assertTrue((where.getWheres().size() == 1));
        Assert.assertTrue(((Condition) (where.getWheres().get(0))).getValue() instanceof ScriptFilter);
        ScriptFilter scriptFilter = (ScriptFilter) (((Condition) (where.getWheres().get(0))).getValue());
        Assert.assertTrue(scriptFilter.getScript().contains("doc['address'].value.split(' ')[0]"));
        Pattern pattern = Pattern.compile("\\(\\(Comparable\\)floor_\\d+\\).compareTo\\(floor_\\d+\\) == 0");
        Matcher matcher = pattern.matcher(scriptFilter.getScript());
        Assert.assertTrue(matcher.find());
    }

    @Test
    public void whereConditionVariableRightVariableEqualTest() throws Exception {

        String query = "SELECT " +
                " * from " +
                TEST_INDEX_ACCOUNT + "/account " +
                " where a = b limit 1000  ";

        SearchDao searchDao = MainTestSuite.getSearchDao() != null ? MainTestSuite.getSearchDao() : getSearchDao();
        System.out.println(searchDao.explain(query).explain().explain());

        Select select = parser.parseSelect((SQLQueryExpr) queryToExpr(query));
        Where where = select.getWhere();
        Assert.assertTrue((where.getWheres().size() == 1));
        Assert.assertTrue(((Condition) (where.getWheres().get(0))).getValue() instanceof ScriptFilter);
        ScriptFilter scriptFilter = (ScriptFilter) (((Condition) (where.getWheres().get(0))).getValue());
        Assert.assertTrue(scriptFilter.getScript().contains("doc['a'].value == doc['b'].value"));
    }

    @Test
    public void concat_ws_fields() throws Exception {

        //here is a bug,csv field with spa
        String query = "SELECT " +
                " concat_ws('-',age,address),address from " +
                TEST_INDEX_ACCOUNT + "/account " +
                " limit 10  ";

        CSVResult csvResult = getCsvResult(false, query);
        List<String> headers = csvResult.getHeaders();
        List<String> contents = csvResult.getLines();
        Assert.assertTrue(headers.size() == 2);
        Assert.assertTrue(contents.get(0).contains("-"));
    }

    @Test
    public void functionLogs() throws Exception {
        String query = "SELECT log10(100) as a, log(1) as b, log(2, 4) as c, log2(8) as d from "
                + TEST_INDEX_ACCOUNT + "/account limit 1";
        CSVResult csvResult = getCsvResult(false, query);
        List<String> content = csvResult.getLines();
        Assert.assertTrue(content.toString().contains("2.0"));
        Assert.assertTrue(content.toString().contains("1.0"));
        Assert.assertTrue(content.toString().contains("0.0"));
        Assert.assertTrue(content.toString().contains("3.0"));
    }

    @Test
    public void functionPow() throws Exception {
        String query = "SELECT pow(account_number, 2) as key,"+
                "abs(age - 60) as new_age from " + TEST_INDEX_ACCOUNT + "/account where _id=25";
        CSVResult csvResult = getCsvResult(false, query);
        List<String> content = csvResult.getLines();
        Assert.assertTrue(content.toString().contains("625"));
        Assert.assertTrue(content.toString().contains("21"));
    }

    @Test
    public void testSumTwoFields() throws Exception {
        String query = "SELECT SUM(account_number+age) AS sum from " + TEST_INDEX_PEOPLE + "/people";
        CSVResult csvResult = getCsvResult(false, query);
        List<String> content = csvResult.getLines();
        Assert.assertTrue(content.toString().contains("752"));
    }

    // todo: change when split is back on language
//    @Test
//    public void split_field() throws Exception {
//
//        //here is a bug,csv field with spa
//        String query = "SELECT " +
//                " split(address,' ')[0],age from " +
//                TestsConstants.TEST_INDEX + "/account where address is not null " +
//                " limit 10  ";
//        SearchDao searchDao = MainTestSuite.getSearchDao() != null ? MainTestSuite.getSearchDao() : getSearchDao();
//        System.out.println(searchDao.explain(query).explain().explain());
//
//        CSVResult csvResult = getCsvResult(false, query);
//        List<String> headers = csvResult.getHeaders();
//        List<String> contents = csvResult.getLines();
//        String[] splits = contents.get(0).split(",");
//        Assert.assertTrue(headers.size() == 2);
//        Assert.assertTrue(Integer.parseInt(splits[0]) > 0);
//    }


    private CSVResult getCsvResult(boolean flat, String query) throws Exception {
        return getCsvResult(flat, query, false, false,false);
    }

    private CSVResult getCsvResult(boolean flat, String query, boolean includeScore, boolean includeType,boolean includeId) throws Exception {
        SearchDao searchDao = MainTestSuite.getSearchDao() != null ? MainTestSuite.getSearchDao() : getSearchDao();
        QueryAction queryAction = searchDao.explain(query);
        Object execution = QueryActionElasticExecutor.executeAnyAction(searchDao.getClient(), queryAction);
        return new CSVResultsExtractor(includeScore, includeType, includeId, false, queryAction).extractResults(execution, flat, ",", false);
    }


    private SearchDao getSearchDao() throws UnknownHostException {
        Settings settings = Settings.builder().put("client.transport.ignore_cluster_name", true).build();
        Client client = new PreBuiltTransportClient(settings).
                addTransportAddress(MainTestSuite.getTransportAddress());
        return new SearchDao(client);
    }
}
