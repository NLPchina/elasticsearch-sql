package org.nlpcn.es4sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.nlpcn.es4sql.domain.*;
import org.nlpcn.es4sql.domain.hints.Hint;
import org.nlpcn.es4sql.domain.hints.HintType;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.parse.ElasticSqlExprParser;
import org.nlpcn.es4sql.parse.SqlParser;

import java.io.IOException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.LinkedList;
import java.util.List;

import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX;

/**
 * Created by Eliran on 21/8/2015.
 */
public class SqlParserTests {
    private static SqlParser parser;

    @BeforeClass
    public static void init(){
        parser = new SqlParser();
    }

    @Test
    public void joinParseCheckSelectedFieldsSplit() throws SqlParseException {
        String query = "SELECT a.firstname ,a.lastname , a.gender ,  d.holdersName ,d.name  FROM elasticsearch-sql_test_index/account a " +
                "LEFT JOIN elasticsearch-sql_test_index/dog d on d.holdersName = a.firstname " +
                " AND d.age < a.age " +
                " WHERE a.firstname = 'eliran' AND " +
                " (a.age > 10 OR a.balance > 2000)"  +
                " AND d.age > 1";

        JoinSelect joinSelect = parser.parseJoinSelect((SQLQueryExpr) queryToExpr(query));

        List<Field> t1Fields = joinSelect.getFirstTable().getSelectedFields();
        Assert.assertEquals(t1Fields.size(),3);
        Assert.assertTrue(fieldExist(t1Fields, "firstname"));
        Assert.assertTrue(fieldExist(t1Fields, "lastname"));
        Assert.assertTrue(fieldExist(t1Fields, "gender"));

        List<Field> t2Fields = joinSelect.getSecondTable().getSelectedFields();
        Assert.assertEquals(t2Fields.size(),2);
        Assert.assertTrue(fieldExist(t2Fields,"holdersName"));
        Assert.assertTrue(fieldExist(t2Fields,"name"));
    }

    @Test
    public void joinParseCheckConnectedFields() throws SqlParseException {
        String query = "SELECT a.firstname ,a.lastname , a.gender ,  d.holdersName ,d.name  FROM elasticsearch-sql_test_index/account a " +
                "LEFT JOIN elasticsearch-sql_test_index/dog d on d.holdersName = a.firstname " +
                " AND d.age < a.age " +
                " WHERE a.firstname = 'eliran' AND " +
                " (a.age > 10 OR a.balance > 2000)"  +
                " AND d.age > 1";

        JoinSelect joinSelect = parser.parseJoinSelect((SQLQueryExpr) queryToExpr(query));

        List<Field> t1Fields = joinSelect.getFirstTable().getConnectedFields();
        Assert.assertEquals(t1Fields.size(),2);
        Assert.assertTrue(fieldExist(t1Fields, "firstname"));
        Assert.assertTrue(fieldExist(t1Fields, "age"));

        List<Field> t2Fields = joinSelect.getSecondTable().getConnectedFields();
        Assert.assertEquals(t2Fields.size(),2);
        Assert.assertTrue(fieldExist(t2Fields,"holdersName"));
        Assert.assertTrue(fieldExist(t2Fields,"age"));
    }

    private boolean fieldExist(List<Field> fields, String fieldName) {
        for(Field field : fields)
            if(field.getName().equals(fieldName)) return true;

        return false;
    }


    @Test
    public void joinParseFromsAreSplitedCorrectly() throws SqlParseException {
        String query = "SELECT a.firstname ,a.lastname , a.gender ,  d.holdersName ,d.name  FROM elasticsearch-sql_test_index/account a " +
                "LEFT JOIN elasticsearch-sql_test_index/dog d on d.holdersName = a.firstname" +
                " WHERE a.firstname = 'eliran' AND " +
                " (a.age > 10 OR a.balance > 2000)"  +
                " AND d.age > 1";

        JoinSelect joinSelect = parser.parseJoinSelect((SQLQueryExpr) queryToExpr(query));
        List<From> t1From = joinSelect.getFirstTable().getFrom();

        Assert.assertNotNull(t1From);
        Assert.assertEquals(1,t1From.size());
        Assert.assertTrue(checkFrom(t1From.get(0),"elasticsearch-sql_test_index","account","a"));

        List<From> t2From = joinSelect.getSecondTable().getFrom();
        Assert.assertNotNull(t2From);
        Assert.assertEquals(1,t2From.size());
        Assert.assertTrue(checkFrom(t2From.get(0),"elasticsearch-sql_test_index","dog","d"));
    }

    private boolean checkFrom(From from, String index, String type, String alias) {
        return from.getAlias().equals(alias) && from.getIndex().equals(index)
                && from.getType().equals(type);
    }

    @Test
    public void joinParseConditionsTestOneCondition() throws SqlParseException {
        String query = "SELECT a.*, a.firstname ,a.lastname , a.gender ,  d.holdersName ,d.name  FROM elasticsearch-sql_test_index/account a " +
                "LEFT JOIN elasticsearch-sql_test_index/dog d on d.holdersName = a.firstname" +
                " WHERE a.firstname = 'eliran' AND " +
                " (a.age > 10 OR a.balance > 2000)"  +
                " AND d.age > 1";

        JoinSelect joinSelect = parser.parseJoinSelect((SQLQueryExpr) queryToExpr(query));
        List<Condition> conditions = joinSelect.getConnectedConditions();
        Assert.assertNotNull(conditions);
        Assert.assertEquals(1,conditions.size());
        Assert.assertTrue("condition not exist: d.holdersName = a.firstname",conditionExist(conditions, "d.holdersName", "a.firstname", Condition.OPEAR.EQ));
    }

    @Test
    public void joinParseConditionsTestTwoConditions() throws SqlParseException {
        String query = "SELECT a.*, a.firstname ,a.lastname , a.gender ,  d.holdersName ,d.name  FROM elasticsearch-sql_test_index/account a " +
                "LEFT JOIN elasticsearch-sql_test_index/dog d on d.holdersName = a.firstname " +
                " AND d.age < a.age " +
                " WHERE a.firstname = 'eliran' AND " +
                " (a.age > 10 OR a.balance > 2000)"  +
                " AND d.age > 1";

        JoinSelect joinSelect = parser.parseJoinSelect((SQLQueryExpr) queryToExpr(query));
        List<Condition> conditions = joinSelect.getConnectedConditions();
        Assert.assertNotNull(conditions);
        Assert.assertEquals(2,conditions.size());
        Assert.assertTrue("condition not exist: d.holdersName = a.firstname",conditionExist(conditions, "d.holdersName", "a.firstname",Condition.OPEAR.EQ));
        Assert.assertTrue("condition not exist: d.age < a.age",conditionExist(conditions, "d.age", "a.age", Condition.OPEAR.LT));
    }


    @Test
    public void joinSplitWhereCorrectly() throws SqlParseException {
        String query = "SELECT a.*, a.firstname ,a.lastname , a.gender ,  d.holdersName ,d.name  FROM elasticsearch-sql_test_index/account a " +
                "LEFT JOIN elasticsearch-sql_test_index/dog d on d.holdersName = a.firstname" +
                " WHERE a.firstname = 'eliran' AND " +
                " (a.age > 10 OR a.balance > 2000)"  +
                " AND d.age > 1";

        JoinSelect joinSelect = parser.parseJoinSelect((SQLQueryExpr) queryToExpr(query));
        String s1Where = joinSelect.getFirstTable().getWhere().toString();
        Assert.assertEquals("AND ( AND firstname EQ eliran, AND ( OR age GT 10, OR balance GT 2000 )  ) " , s1Where);
        String s2Where = joinSelect.getSecondTable().getWhere().toString();
        Assert.assertEquals("AND age GT 1",s2Where);
    }

    @Test
    public void joinConditionWithComplexObjectComparisonRightSide() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        String query = String.format("select c.name.firstname,c.parents.father , h.name,h.words from %s/gotCharacters c " +
                "JOIN %s/gotHouses h " +
                "on h.name = c.name.lastname  " +
                "where c.name.firstname='Daenerys'", TEST_INDEX,TEST_INDEX);
        JoinSelect joinSelect = parser.parseJoinSelect((SQLQueryExpr) queryToExpr(query));
        List<Condition> conditions = joinSelect.getConnectedConditions();
        Assert.assertNotNull(conditions);
        Assert.assertEquals(1,conditions.size());
        Assert.assertTrue("condition not exist: h.name = c.name.lastname",conditionExist(conditions, "h.name", "c.name.lastname", Condition.OPEAR.EQ));
    }

    @Test
    public void joinConditionWithComplexObjectComparisonLeftSide() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        String query = String.format("select c.name.firstname,c.parents.father , h.name,h.words from %s/gotCharacters c " +
                "JOIN %s/gotHouses h " +
                "on c.name.lastname = h.name  " +
                "where c.name.firstname='Daenerys'", TEST_INDEX,TEST_INDEX);
        JoinSelect joinSelect = parser.parseJoinSelect((SQLQueryExpr) queryToExpr(query));
        List<Condition> conditions = joinSelect.getConnectedConditions();
        Assert.assertNotNull(conditions);
        Assert.assertEquals(1,conditions.size());
        Assert.assertTrue("condition not exist: c.name.lastname = h.name",conditionExist(conditions, "c.name.lastname", "h.name", Condition.OPEAR.EQ));
    }


    @Test
    public void limitHintsOnJoin() throws SqlParseException {
        String query = String.format("select /*! JOIN_TABLES_LIMIT(1000,null) */ c.name.firstname,c.parents.father , h.name,h.words from %s/gotCharacters c " +
                "use KEY (termsFilter) "+
                "JOIN %s/gotHouses h " +
                "on c.name.lastname = h.name  " +
                "where c.name.firstname='Daenerys'", TEST_INDEX,TEST_INDEX);
        JoinSelect joinSelect = parser.parseJoinSelect((SQLQueryExpr) queryToExpr(query));
        List<Hint> hints = joinSelect.getHints();
        Assert.assertNotNull(hints);
        Assert.assertEquals("hints size was not 1", 1, hints.size());
        Hint hint  = hints.get(0);
        Assert.assertEquals(HintType.JOIN_LIMIT,hint.getType());
        Object[] params = hint.getParams();
        Assert.assertNotNull(params);
        Assert.assertEquals("params size was not 2", 2, params.length);
        Assert.assertEquals(1000,params[0]);
        Assert.assertEquals(null,params[1]);
    }

    @Test
    public void hashTermsFilterHint() throws SqlParseException {
        String query = String.format("select /*! HASH_WITH_TERMS_FILTER*/ c.name.firstname,c.parents.father , h.name,h.words from %s/gotCharacters c " +
                "use KEY (termsFilter) "+
                "JOIN %s/gotHouses h " +
                "on c.name.lastname = h.name  " +
                "where c.name.firstname='Daenerys'", TEST_INDEX,TEST_INDEX);
        JoinSelect joinSelect = parser.parseJoinSelect((SQLQueryExpr) queryToExpr(query));
        List<Hint> hints = joinSelect.getHints();
        Assert.assertNotNull(hints);
        Assert.assertEquals("hints size was not 1", 1, hints.size());
        Hint hint  = hints.get(0);
        Assert.assertEquals(HintType.HASH_WITH_TERMS_FILTER,hint.getType());
    }

    @Test
    public void multipleHints() throws SqlParseException {
        String query = String.format("select /*! HASH_WITH_TERMS_FILTER*/ /*! JOIN_TABLES_LIMIT(1000,null) */ " +
                " /*! JOIN_TABLES_LIMIT(100,200) */ " +
                "c.name.firstname,c.parents.father , h.name,h.words from %s/gotCharacters c " +
                "use KEY (termsFilter) "+
                "JOIN %s/gotHouses h " +
                "on c.name.lastname = h.name  " +
                "where c.name.firstname='Daenerys'", TEST_INDEX,TEST_INDEX);

        JoinSelect joinSelect = parser.parseJoinSelect((SQLQueryExpr) queryToExpr(query));
        List<Hint> hints = joinSelect.getHints();

        Assert.assertNotNull(hints);
        Assert.assertEquals("hints size was not 3", 3, hints.size());
        Hint firstHint  = hints.get(0);
        Assert.assertEquals(HintType.HASH_WITH_TERMS_FILTER, firstHint.getType());
        Hint secondHint  = hints.get(1);
        Assert.assertEquals(HintType.JOIN_LIMIT, secondHint.getType());
        Assert.assertEquals(1000,secondHint.getParams()[0]);
        Assert.assertEquals(null,secondHint.getParams()[1]);
        Hint thirdHint  = hints.get(2);
        Assert.assertEquals(100,thirdHint.getParams()[0]);
        Assert.assertEquals(200,thirdHint.getParams()[1]);
        Assert.assertEquals(HintType.JOIN_LIMIT, thirdHint.getType());
    }

    @Test
    public void searchWithOdbcTimeFormatParse() throws SqlParseException {
        String query = String.format("SELECT insert_time FROM %s/odbc WHERE insert_time < {ts '2015-03-15 00:00:00.000'}", TEST_INDEX);
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        LinkedList<Where> wheres = select.getWhere().getWheres();
        Assert.assertEquals(1,wheres.size());
        Condition condition = (Condition) wheres.get(0);
        Assert.assertEquals("{ts '2015-03-15 00:00:00.000'}",condition.getValue().toString());

    }

    @Test
    public void indexWithSpacesWithinBrackets() throws SqlParseException {
        String query = "SELECT insert_time FROM [Test Index] WHERE age > 3";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<From> fromList = select.getFrom();
        Assert.assertEquals(1, fromList.size());
        From from = fromList.get(0);
        Assert.assertEquals("Test Index",from.getIndex());
    }

    @Test
    public void indexWithSpacesWithTypeWithinBrackets() throws SqlParseException {
        String query = "SELECT insert_time FROM [Test Index]/type1 WHERE age > 3";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<From> fromList = select.getFrom();
        Assert.assertEquals(1, fromList.size());
        From from = fromList.get(0);
        Assert.assertEquals("Test Index",from.getIndex());
        Assert.assertEquals("type1",from.getType());
    }


    @Test
    public void fieldWithSpacesWithinBrackets() throws SqlParseException {
        String query = "SELECT insert_time FROM name/type1 WHERE [first name] = 'Name'";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<Where> where = select.getWhere().getWheres();
        Assert.assertEquals(1,where.size());
        Condition condition = (Condition) where.get(0);
        Assert.assertEquals("first name",condition.getName());
        Assert.assertEquals("Name",condition.getValue());
    }

    @Test
    public void twoIndices() throws SqlParseException {
        String query = "SELECT insert_time FROM index1/type1 , index2/type2 WHERE age > 3";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<From> fromList = select.getFrom();
        Assert.assertEquals(2, fromList.size());
        From from1 = fromList.get(0);
        From from2 = fromList.get(1);
        boolean preservedOrder = from1.getIndex().equals("index1") && from1.getType().equals("type1")
                && from2.getIndex().equals("index2") && from2.getType().equals("type2");
        boolean notPreservedOrder = from1.getIndex().equals("index2")  && from1.getType().equals("type2")
                && from2.getIndex().equals("index1") && from2.getType().equals("type1");
        Assert.assertTrue(preservedOrder || notPreservedOrder);
    }

    @Test
    public void fieldWithATcharAtWhere() throws SqlParseException {
        String query = "SELECT * FROM index/type where @field = 6 ";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        LinkedList<Where> wheres = select.getWhere().getWheres();
        Assert.assertEquals(1,wheres.size());
        Condition condition = (Condition) wheres.get(0);
        Assert.assertEquals("@field", condition.getName());
    }

    @Test
    public void fieldWithATcharAtSelect() throws SqlParseException {
        String query = "SELECT @field FROM index/type where field2 = 6 ";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<Field> fields = select.getFields();
        Assert.assertEquals(1,fields.size());
        Field field = fields.get(0);
        Assert.assertEquals(field.getName(),"@field");
    }

    @Test
    public void fieldWithATcharAtSelectOnAgg() throws SqlParseException {
        String query = "SELECT max(@field) FROM index/type where field2 = 6 ";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<Field> fields = select.getFields();
        Assert.assertEquals(1,fields.size());
        Field field = fields.get(0);
        Assert.assertEquals("MAX(@field)",field.toString());
    }

    @Test
    public void fieldWithColonCharAtSelect() throws SqlParseException {
        String query = "SELECT a:b FROM index/type where field2 = 6 ";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<Field> fields = select.getFields();
        Assert.assertEquals(1,fields.size());
        Field field = fields.get(0);
        Assert.assertEquals(field.getName(),"a:b");
    }

    @Test
    public void fieldWithColonCharAtWhere() throws SqlParseException {
        String query = "SELECT * FROM index/type where a:b = 6 ";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        LinkedList<Where> wheres = select.getWhere().getWheres();
        Assert.assertEquals(1,wheres.size());
        Condition condition = (Condition) wheres.get(0);
        Assert.assertEquals("a:b", condition.getName());
    }

    @Test
    public void fieldIsNull() throws SqlParseException {
        String query = "SELECT * FROM index/type where a IS NOT NULL";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        LinkedList<Where> wheres = select.getWhere().getWheres();
        Assert.assertEquals(1,wheres.size());
        Condition condition = (Condition) wheres.get(0);
        Assert.assertEquals("a", condition.getName());
        Assert.assertNull(condition.getValue());
    }

    @Test
    public void innerQueryTest() throws SqlParseException {
        String query = String.format("select * from %s/dog where holdersName IN (select firstname from %s/account where firstname = 'eliran')",TEST_INDEX,TEST_INDEX);
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        Assert.assertTrue(select.containsSubQueries());
        Assert.assertEquals(1,select.getSubQueries().size());
    }

    @Test
    public void inTermsSubQueryTest() throws SqlParseException {
        String query = String.format("select * from %s/dog where holdersName = IN_TERMS (select firstname from %s/account where firstname = 'eliran')",TEST_INDEX,TEST_INDEX);
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        Assert.assertTrue(select.containsSubQueries());
        Assert.assertEquals(1,select.getSubQueries().size());
    }


    @Test
    public void innerQueryTestTwoQueries() throws SqlParseException {
        String query = String.format("select * from %s/dog where holdersName IN (select firstname from %s/account where firstname = 'eliran') and age IN (select name.ofHisName from %s/gotCharacters) ",TEST_INDEX,TEST_INDEX,TEST_INDEX);
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        Assert.assertTrue(select.containsSubQueries());
        Assert.assertEquals(2,select.getSubQueries().size());
    }

    @Test
    public void indexWithDotsAndHyphen() throws SqlParseException {
        String query = "select * from data-2015.08.22";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        Assert.assertEquals(1,select.getFrom().size());
        Assert.assertEquals("data-2015.08.22",select.getFrom().get(0).getIndex());
    }

    private SQLExpr queryToExpr(String query) {
        return new ElasticSqlExprParser(query).expr();
    }

    private boolean conditionExist(List<Condition> conditions, String from, String to, Condition.OPEAR opear) {
        String[] aliasAndField = to.split("\\.",2);
        String toAlias = aliasAndField[0];
        String toField = aliasAndField[1];
        for (Condition condition : conditions){
            if(condition.getOpear() !=  opear) continue;

            boolean fromIsEqual = condition.getName().equals(from);
            if(!fromIsEqual) continue;

            String[] valueAliasAndField = condition.getValue().toString().split("\\.",2);
            boolean toFieldNameIsEqual = valueAliasAndField[1].equals(toField);
            boolean toAliasIsEqual =  valueAliasAndField[0].equals(toAlias);
            boolean toIsEqual = toAliasIsEqual && toFieldNameIsEqual;

            if(toIsEqual) return true;
        }
        return false;
    }


}
