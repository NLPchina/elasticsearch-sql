package org.nlpcn.es4sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.nlpcn.es4sql.domain.*;
import org.nlpcn.es4sql.domain.hints.Hint;
import org.nlpcn.es4sql.domain.hints.HintType;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.parse.ElasticSqlExprParser;
import org.nlpcn.es4sql.parse.FieldMaker;
import org.nlpcn.es4sql.parse.ScriptFilter;
import org.nlpcn.es4sql.parse.SqlParser;
import org.nlpcn.es4sql.query.maker.FilterMaker;

import java.io.IOException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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

    @Test
    public void indexWithSemiColons() throws SqlParseException {
        String query = "select * from some;index";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        Assert.assertEquals(1,select.getFrom().size());
        Assert.assertEquals("some;index",select.getFrom().get(0).getIndex());
    }

    @Test
    public void scriptFiledPlusLiteralTest() throws SqlParseException {
        String query = "SELECT field1 + 3 FROM index/type";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<Field> fields = select.getFields();
        Assert.assertEquals(1,fields.size());
        Field field = fields.get(0);
        Assert.assertTrue(field instanceof MethodField);
        MethodField scriptMethod = (MethodField) field;
        Assert.assertEquals("script",scriptMethod.getName().toLowerCase());
        Assert.assertEquals(2,scriptMethod.getParams().size());
        Assert.assertEquals("field1 + 3" ,scriptMethod.getParams().get(0).toString());
        Assert.assertEquals("doc['field1'].value + 3" ,scriptMethod.getParams().get(1).toString());
    }

    @Test
    public void scriptFieldPlusFieldTest() throws SqlParseException {
        String query = "SELECT field1 + field2 FROM index/type";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<Field> fields = select.getFields();
        Assert.assertEquals(1,fields.size());
        Field field = fields.get(0);
        Assert.assertTrue(field instanceof MethodField);
        MethodField scriptMethod = (MethodField) field;
        Assert.assertEquals("script",scriptMethod.getName().toLowerCase());
        Assert.assertEquals(2,scriptMethod.getParams().size());
        Assert.assertEquals("field1 + field2" ,scriptMethod.getParams().get(0).toString());
        Assert.assertEquals("doc['field1'].value + doc['field2'].value" ,scriptMethod.getParams().get(1).toString());
    }


    @Test
    public void scriptLiteralPlusLiteralTest() throws SqlParseException {
        String query = "SELECT 1 + 2  FROM index/type";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<Field> fields = select.getFields();
        Assert.assertEquals(1,fields.size());
        Field field = fields.get(0);
        Assert.assertTrue(field instanceof MethodField);
        MethodField scriptMethod = (MethodField) field;
        Assert.assertEquals("script",scriptMethod.getName().toLowerCase());
        Assert.assertEquals(2,scriptMethod.getParams().size());
        Assert.assertEquals("1 + 2" ,scriptMethod.getParams().get(0).toString());
        Assert.assertEquals("1 + 2" ,scriptMethod.getParams().get(1).toString());
    }

    @Test
    public void scriptFieldPlusFieldWithAliasTest() throws SqlParseException {
        String query = "SELECT field1 + field2 as myfield FROM index/type";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<Field> fields = select.getFields();
        Assert.assertEquals(1,fields.size());
        Field field = fields.get(0);
        Assert.assertTrue(field instanceof MethodField);
        MethodField scriptMethod = (MethodField) field;
        Assert.assertEquals("script",scriptMethod.getName().toLowerCase());
        Assert.assertEquals(2,scriptMethod.getParams().size());
        Assert.assertEquals("myfield" ,scriptMethod.getParams().get(0).toString());
        Assert.assertEquals("doc['field1'].value + doc['field2'].value" ,scriptMethod.getParams().get(1).toString());
    }


    @Test
    public void explicitScriptOnAggregation() throws SqlParseException {
        String query = "SELECT avg( script('add','doc[\\'field1\\'].value + doc[\\'field2\\'].value') ) FROM index/type";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<Field> fields = select.getFields();
        Assert.assertEquals(1,fields.size());
        Field field = fields.get(0);
        Assert.assertTrue(field instanceof MethodField);
        MethodField avgMethodField = (MethodField) field;
        Assert.assertEquals("avg",avgMethodField.getName().toLowerCase());
        Assert.assertEquals(1,avgMethodField.getParams().size());
        MethodField scriptMethod = (MethodField) avgMethodField.getParams().get(0).value;
        Assert.assertEquals("script",scriptMethod.getName().toLowerCase());
        Assert.assertEquals(2,scriptMethod.getParams().size());
        Assert.assertEquals("doc['field1'].value + doc['field2'].value" ,scriptMethod.getParams().get(1).toString());
    }

    @Test
    public void implicitScriptOnAggregation() throws SqlParseException {
        String query = "SELECT avg(field1 + field2) FROM index/type";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<Field> fields = select.getFields();
        Assert.assertEquals(1,fields.size());
        Field field = fields.get(0);
        Assert.assertTrue(field instanceof MethodField);
        MethodField avgMethodField = (MethodField) field;
        Assert.assertEquals("avg",avgMethodField.getName().toLowerCase());
        Assert.assertEquals(1,avgMethodField.getParams().size());
        MethodField scriptMethod = (MethodField) avgMethodField.getParams().get(0).value;
        Assert.assertEquals("script",scriptMethod.getName().toLowerCase());
        Assert.assertEquals(2,scriptMethod.getParams().size());
        Assert.assertEquals("doc['field1'].value + doc['field2'].value" ,scriptMethod.getParams().get(1).toString());
    }

    @Test
    public void nestedFieldOnWhereNoPathSimpleField() throws SqlParseException {
        String query = "select * from myIndex where nested(message.name) = 'hey'";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        Where where = select.getWhere().getWheres().get(0);
        Assert.assertTrue("where should be condition", where instanceof Condition);
        Condition condition = (Condition) where;
        Assert.assertTrue("condition should be nested",condition.isNested());
        Assert.assertEquals("message",condition.getNestedPath());
        Assert.assertEquals("message.name",condition.getName());
    }


    @Test
    public void nestedFieldOnWhereNoPathComplexField() throws SqlParseException {
        String query = "select * from myIndex where nested(message.moreNested.name) = 'hey'";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        Where where = select.getWhere().getWheres().get(0);
        Assert.assertTrue("where should be condition", where instanceof Condition);
        Condition condition = (Condition) where;
        Assert.assertTrue("condition should be nested",condition.isNested());
        Assert.assertEquals("message.moreNested",condition.getNestedPath());
        Assert.assertEquals("message.moreNested.name",condition.getName());
    }

    @Test
    public void nestedFieldOnWhereGivenPath() throws SqlParseException {
        String query = "select * from myIndex where nested(message.name,message) = 'hey'";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        Where where = select.getWhere().getWheres().get(0);
        Assert.assertTrue("where should be condition", where instanceof Condition);
        Condition condition = (Condition) where;
        Assert.assertTrue("condition should be nested",condition.isNested());
        Assert.assertEquals("message",condition.getNestedPath());
        Assert.assertEquals("message.name",condition.getName());
    }

    @Test
    public void nestedFieldOnGroupByNoPath() throws SqlParseException {
        String query = "select * from myIndex group by nested(message.name)";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        Field field = select.getGroupBys().get(0).get(0);
        Assert.assertTrue("condition should be nested",field.isNested());
        Assert.assertEquals("message",field.getNestedPath());
        Assert.assertEquals("message.name",field.getName());
    }

    @Test
    public void nestedFieldOnGroupByWithPath() throws SqlParseException {
        String query = "select * from myIndex group by nested(message.name,message)";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        Field field = select.getGroupBys().get(0).get(0);
        Assert.assertTrue("condition should be nested",field.isNested());
        Assert.assertEquals("message",field.getNestedPath());
        Assert.assertEquals("message.name",field.getName());
    }

    @Test
    public void filterAggTestNoAlias() throws SqlParseException {
        String query = "select * from myIndex group by a , filter(  a > 3 AND b='3' )";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<List<Field>> groupBys = select.getGroupBys();
        Assert.assertEquals(1,groupBys.size());
        Field aAgg = groupBys.get(0).get(0);
        Assert.assertEquals("a",aAgg.getName());
        Field field = groupBys.get(0).get(1);
        Assert.assertTrue("filter field should be method field",field instanceof MethodField);
        MethodField filterAgg = (MethodField) field;
        Assert.assertEquals("filter", filterAgg.getName());
        Map<String, Object> params = filterAgg.getParamsAsMap();
        Assert.assertEquals(2, params.size());
        Object alias = params.get("alias");
        Assert.assertEquals("filter(a > 3 AND b = '3')@FILTER",alias);

        Assert.assertTrue(params.get("where") instanceof Where);
        Where where  = (Where) params.get("where");
        Assert.assertEquals(2,where.getWheres().size());
    }

    @Test
    public void filterAggTestWithAlias() throws SqlParseException {
        String query = "select * from myIndex group by a , filter(myFilter, a > 3 AND b='3' )";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<List<Field>> groupBys = select.getGroupBys();
        Assert.assertEquals(1,groupBys.size());
        Field aAgg = groupBys.get(0).get(0);
        Assert.assertEquals("a",aAgg.getName());
        Field field = groupBys.get(0).get(1);
        Assert.assertTrue("filter field should be method field",field instanceof MethodField);
        MethodField filterAgg = (MethodField) field;
        Assert.assertEquals("filter", filterAgg.getName());
        Map<String, Object> params = filterAgg.getParamsAsMap();
        Assert.assertEquals(2, params.size());
        Object alias = params.get("alias");
        Assert.assertEquals("myFilter@FILTER",alias);

        Assert.assertTrue(params.get("where") instanceof Where);
        Where where  = (Where) params.get("where");
        Assert.assertEquals(2,where.getWheres().size());
    }


    @Test
    public void filterAggTestWithAliasAsString() throws SqlParseException {
        String query = "select * from myIndex group by a , filter('my filter', a > 3 AND b='3' )";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<List<Field>> groupBys = select.getGroupBys();
        Assert.assertEquals(1,groupBys.size());
        Field aAgg = groupBys.get(0).get(0);
        Assert.assertEquals("a",aAgg.getName());
        Field field = groupBys.get(0).get(1);
        Assert.assertTrue("filter field should be method field",field instanceof MethodField);
        MethodField filterAgg = (MethodField) field;
        Assert.assertEquals("filter", filterAgg.getName());
        Map<String, Object> params = filterAgg.getParamsAsMap();
        Assert.assertEquals(2, params.size());
        Object alias = params.get("alias");
        Assert.assertEquals("my filter@FILTER",alias);

        Assert.assertTrue(params.get("where") instanceof Where);
        Where where  = (Where) params.get("where");
        Assert.assertEquals(2,where.getWheres().size());
    }
    @Test
    public void doubleOrderByTest() throws SqlParseException {
        String query = "select * from indexName order by a asc, b desc";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<Order> orderBys = select.getOrderBys();
        Assert.assertEquals(2,orderBys.size());
        Assert.assertEquals("a",orderBys.get(0).getName());
        Assert.assertEquals("ASC",orderBys.get(0).getType());

        Assert.assertEquals("b",orderBys.get(1).getName());
        Assert.assertEquals("DESC",orderBys.get(1).getType());
    }

    @Test
    public void parseJoinWithOneTableOrderByAttachToCorrectTable() throws SqlParseException {
        String query = String.format("select c.name.firstname , d.words from %s/gotCharacters c " +
                "JOIN %s/gotHouses d on d.name = c.house " +
                "order by c.name.firstname"
                ,  TEST_INDEX, TEST_INDEX);

        JoinSelect joinSelect = parser.parseJoinSelect((SQLQueryExpr) queryToExpr(query));
        Assert.assertTrue("first table should be ordered",joinSelect.getFirstTable().isOrderdSelect());
        Assert.assertFalse("second table should not be ordered", joinSelect.getSecondTable().isOrderdSelect());

    }

    @Test
    public void parseJoinWithOneTableOrderByRemoveAlias() throws SqlParseException {
        String query = String.format("select c.name.firstname , d.words from %s/gotCharacters c " +
                "JOIN %s/gotHouses d on d.name = c.house " +
                "order by c.name.firstname"
                ,  TEST_INDEX, TEST_INDEX);

        JoinSelect joinSelect = parser.parseJoinSelect((SQLQueryExpr) queryToExpr(query));
        List<Order> orderBys = joinSelect.getFirstTable().getOrderBys();
        Assert.assertEquals(1,orderBys.size());
        Order order = orderBys.get(0);
        Assert.assertEquals("name.firstname", order.getName());

    }

    @Test
    public void termsWithStringTest() throws SqlParseException {
        String query = "select * from x where y = IN_TERMS('a','b')";
        Select select = parser.parseSelect((SQLQueryExpr) queryToExpr(query));
        Condition condition = (Condition) select.getWhere().getWheres().get(0);
        Object[] values = (Object[]) condition.getValue();
        Assert.assertEquals("a",values[0]);
        Assert.assertEquals("b",values[1]);
    }

    @Test
    public void termWithStringTest() throws SqlParseException {
        String query = "select * from x where y = TERM('a')";
        Select select = parser.parseSelect((SQLQueryExpr) queryToExpr(query));
        Condition condition = (Condition) select.getWhere().getWheres().get(0);
        Object[] values = (Object[]) condition.getValue();
        Assert.assertEquals("a",values[0]);
    }

    @Test
    public void complexNestedTest() throws SqlParseException {
        String query = "select * from x where nested('y',y.b = 'a' and y.c  = 'd') ";
        Select select = parser.parseSelect((SQLQueryExpr) queryToExpr(query));
        Condition condition = (Condition) select.getWhere().getWheres().get(0);
        Assert.assertEquals(Condition.OPEAR.NESTED_COMPLEX,condition.getOpear());
        Assert.assertEquals("y",condition.getName());
        Assert.assertTrue(condition.getValue() instanceof Where);
        Where where = (Where) condition.getValue();
        Assert.assertEquals(2,where.getWheres().size());
    }

    @Test
    public void scriptOnFilterNoParams() throws SqlParseException {
        String query = "select * from x where script('doc[\\'field\\'].date.hourOfDay == 3') ";
        Select select = parser.parseSelect((SQLQueryExpr) queryToExpr(query));
        Condition condition = (Condition) select.getWhere().getWheres().get(0);
        Assert.assertEquals(Condition.OPEAR.SCRIPT,condition.getOpear());
        Assert.assertEquals(null,condition.getName());
        Assert.assertTrue(condition.getValue() instanceof ScriptFilter);
        ScriptFilter scriptFilter = (ScriptFilter) condition.getValue();
        Assert.assertEquals("doc['field'].date.hourOfDay == 3",scriptFilter.getScript());
        Assert.assertFalse(scriptFilter.containsParameters());
    }

    @Test
    public void scriptOnFilterWithParams() throws SqlParseException {
        String query = "select * from x where script('doc[\\'field\\'].date.hourOfDay == x','x'=3) ";
        Select select = parser.parseSelect((SQLQueryExpr) queryToExpr(query));
        Condition condition = (Condition) select.getWhere().getWheres().get(0);
        Assert.assertEquals(Condition.OPEAR.SCRIPT,condition.getOpear());
        Assert.assertEquals(null,condition.getName());
        Assert.assertTrue(condition.getValue() instanceof ScriptFilter);
        ScriptFilter scriptFilter = (ScriptFilter) condition.getValue();
        Assert.assertEquals("doc['field'].date.hourOfDay == x",scriptFilter.getScript());
        Assert.assertTrue(scriptFilter.containsParameters());
        Map<String, Object> args = scriptFilter.getArgs();
        Assert.assertEquals(1, args.size());
        Assert.assertTrue(args.containsKey("x"));
        Assert.assertEquals(3, args.get("x"));

    }

    @Test
    public void fieldsAsNumbersOnWhere() throws SqlParseException {
        String query = "select * from x where ['3'] > 2";
        Select select = parser.parseSelect((SQLQueryExpr) queryToExpr(query));
        LinkedList<Where> wheres = select.getWhere().getWheres();
        Assert.assertEquals(1, wheres.size());
        Where where = wheres.get(0);
        Assert.assertEquals(Condition.class,where.getClass());
        Condition condition = (Condition) where;
        Assert.assertEquals("3", condition.getName());
    }

    @Test
    public void likeTestWithEscaped() throws SqlParseException {
        String query = "select * from x where name like '&UNDERSCOREhey_%&PERCENT'";
        Select select = parser.parseSelect((SQLQueryExpr) queryToExpr(query));
        BoolFilterBuilder explan = FilterMaker.explan(select.getWhere());
        String filterAsString = explan.toString();
        Assert.assertTrue(filterAsString.contains("_hey?*%"));
    }


    @Test
    public void complexNestedAndOtherQuery() throws SqlParseException {
        String query = "select * from x where nested('path',path.x=3) and y=3";
        Select select = parser.parseSelect((SQLQueryExpr) queryToExpr(query));
        LinkedList<Where> wheres = select.getWhere().getWheres();
        Assert.assertEquals(2, wheres.size());
        Assert.assertEquals("AND path NESTED_COMPLEX AND ( AND path.x EQ 3 ) ",wheres.get(0).toString());
        Assert.assertEquals("AND y EQ 3",wheres.get(1).toString());
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
