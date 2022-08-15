package org.nlpcn.es4sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.statement.SQLUnionOperator;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.nlpcn.es4sql.domain.Condition;
import org.nlpcn.es4sql.domain.Field;
import org.nlpcn.es4sql.domain.From;
import org.nlpcn.es4sql.domain.JoinSelect;
import org.nlpcn.es4sql.domain.MethodField;
import org.nlpcn.es4sql.domain.Order;
import org.nlpcn.es4sql.domain.Select;
import org.nlpcn.es4sql.domain.Where;
import org.nlpcn.es4sql.domain.hints.Hint;
import org.nlpcn.es4sql.domain.hints.HintType;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.parse.ElasticSqlExprParser;
import org.nlpcn.es4sql.parse.ScriptFilter;
import org.nlpcn.es4sql.parse.SqlParser;
import org.nlpcn.es4sql.query.maker.QueryMaker;
import org.nlpcn.es4sql.query.multi.MultiQuerySelect;

import java.io.IOException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_DOG;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_GAME_OF_THRONES;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX_ODBC;

/**
 * Created by Eliran on 21/8/2015.
 */
public class SqlParserTests {
    private static SqlParser parser;

    @BeforeClass
    public static void init() {
        parser = new SqlParser();
    }

    @Test
    public void joinParseCheckSelectedFieldsSplit() throws SqlParseException {
        String query = "SELECT a.firstname ,a.lastname , a.gender ,  d.holdersName ,d.name  FROM " +
                TEST_INDEX_ACCOUNT +
                "/account a " +
                "LEFT JOIN " +
                TEST_INDEX_DOG +
                "/dog d on d.holdersName = a.firstname " +
                " AND d.age < a.age " +
                " WHERE a.firstname = 'eliran' AND " +
                " (a.age > 10 OR a.balance > 2000)" +
                " AND d.age > 1";

        JoinSelect joinSelect = parser.parseJoinSelect((SQLQueryExpr) queryToExpr(query));

        List<Field> t1Fields = joinSelect.getFirstTable().getSelectedFields();
        Assert.assertEquals(t1Fields.size(), 3);
        Assert.assertTrue(fieldExist(t1Fields, "firstname"));
        Assert.assertTrue(fieldExist(t1Fields, "lastname"));
        Assert.assertTrue(fieldExist(t1Fields, "gender"));

        List<Field> t2Fields = joinSelect.getSecondTable().getSelectedFields();
        Assert.assertEquals(t2Fields.size(), 2);
        Assert.assertTrue(fieldExist(t2Fields, "holdersName"));
        Assert.assertTrue(fieldExist(t2Fields, "name"));
    }

    @Test
    public void joinParseCheckConnectedFields() throws SqlParseException {
        String query = "SELECT a.firstname ,a.lastname , a.gender ,  d.holdersName ,d.name  FROM " +
                TEST_INDEX_ACCOUNT +
                "/account a " +
                "LEFT JOIN " +
                TEST_INDEX_DOG +
                "/dog d on d.holdersName = a.firstname " +
                " AND d.age < a.age " +
                " WHERE a.firstname = 'eliran' AND " +
                " (a.age > 10 OR a.balance > 2000)" +
                " AND d.age > 1";

        JoinSelect joinSelect = parser.parseJoinSelect((SQLQueryExpr) queryToExpr(query));

        List<Field> t1Fields = joinSelect.getFirstTable().getConnectedFields();
        Assert.assertEquals(t1Fields.size(), 2);
        Assert.assertTrue(fieldExist(t1Fields, "firstname"));
        Assert.assertTrue(fieldExist(t1Fields, "age"));

        List<Field> t2Fields = joinSelect.getSecondTable().getConnectedFields();
        Assert.assertEquals(t2Fields.size(), 2);
        Assert.assertTrue(fieldExist(t2Fields, "holdersName"));
        Assert.assertTrue(fieldExist(t2Fields, "age"));
    }

    private boolean fieldExist(List<Field> fields, String fieldName) {
        for (Field field : fields)
            if (field.getName().equals(fieldName)) return true;

        return false;
    }


    @Test
    public void joinParseFromsAreSplitedCorrectly() throws SqlParseException {
        String query = "SELECT a.firstname ,a.lastname , a.gender ,  d.holdersName ,d.name  FROM " +
                TEST_INDEX_ACCOUNT +
                "/account a " +
                "LEFT JOIN " +
                TEST_INDEX_DOG +
                "/dog d on d.holdersName = a.firstname" +
                " WHERE a.firstname = 'eliran' AND " +
                " (a.age > 10 OR a.balance > 2000)" +
                " AND d.age > 1";

        JoinSelect joinSelect = parser.parseJoinSelect((SQLQueryExpr) queryToExpr(query));
        List<From> t1From = joinSelect.getFirstTable().getFrom();

        Assert.assertNotNull(t1From);
        Assert.assertEquals(1, t1From.size());
        Assert.assertTrue(checkFrom(t1From.get(0), TEST_INDEX_ACCOUNT, "account", "a"));

        List<From> t2From = joinSelect.getSecondTable().getFrom();
        Assert.assertNotNull(t2From);
        Assert.assertEquals(1, t2From.size());
        Assert.assertTrue(checkFrom(t2From.get(0), TEST_INDEX_DOG, "dog", "d"));
    }

    private boolean checkFrom(From from, String index, String type, String alias) {
        return from.getAlias().equals(alias) && from.getIndex().equals(index)
                && from.getType().equals(type);
    }

    @Test
    public void joinParseConditionsTestOneCondition() throws SqlParseException {
        String query = "SELECT a.*, a.firstname ,a.lastname , a.gender ,  d.holdersName ,d.name  FROM " +
                TEST_INDEX_ACCOUNT +
                "/account a " +
                "LEFT JOIN " +
                TEST_INDEX_DOG +
                "/dog d on d.holdersName = a.firstname" +
                " WHERE a.firstname = 'eliran' AND " +
                " (a.age > 10 OR a.balance > 2000)" +
                " AND d.age > 1";

        JoinSelect joinSelect = parser.parseJoinSelect((SQLQueryExpr) queryToExpr(query));
        List<Condition> conditions = joinSelect.getConnectedConditions();
        Assert.assertNotNull(conditions);
        Assert.assertEquals(1, conditions.size());
        Assert.assertTrue("condition not exist: d.holdersName = a.firstname", conditionExist(conditions, "d.holdersName", "a.firstname", Condition.OPEAR.EQ));
    }

    @Test
    public void joinParseConditionsTestTwoConditions() throws SqlParseException {
        String query = "SELECT a.*, a.firstname ,a.lastname , a.gender ,  d.holdersName ,d.name  FROM " +
                TEST_INDEX_ACCOUNT +
                "/account a " +
                "LEFT JOIN " +
                TEST_INDEX_DOG +
                "/dog d on d.holdersName = a.firstname " +
                " AND d.age < a.age " +
                " WHERE a.firstname = 'eliran' AND " +
                " (a.age > 10 OR a.balance > 2000)" +
                " AND d.age > 1";

        JoinSelect joinSelect = parser.parseJoinSelect((SQLQueryExpr) queryToExpr(query));
        List<Condition> conditions = joinSelect.getConnectedConditions();
        Assert.assertNotNull(conditions);
        Assert.assertEquals(2, conditions.size());
        Assert.assertTrue("condition not exist: d.holdersName = a.firstname", conditionExist(conditions, "d.holdersName", "a.firstname", Condition.OPEAR.EQ));
        Assert.assertTrue("condition not exist: d.age < a.age", conditionExist(conditions, "d.age", "a.age", Condition.OPEAR.LT));
    }


    @Test
    public void joinSplitWhereCorrectly() throws SqlParseException {
        String query = "SELECT a.*, a.firstname ,a.lastname , a.gender ,  d.holdersName ,d.name  FROM " +
                TEST_INDEX_ACCOUNT +
                "/account a " +
                "LEFT JOIN " +
                TEST_INDEX_DOG +
                "/dog d on d.holdersName = a.firstname" +
                " WHERE a.firstname = 'eliran' AND " +
                " (a.age > 10 OR a.balance > 2000)" +
                " AND d.age > 1";

        JoinSelect joinSelect = parser.parseJoinSelect((SQLQueryExpr) queryToExpr(query));
        String s1Where = joinSelect.getFirstTable().getWhere().toString();
        Assert.assertEquals("AND ( AND firstname EQ eliran, AND ( OR age GT 10, OR balance GT 2000 )  ) ", s1Where);
        String s2Where = joinSelect.getSecondTable().getWhere().toString();
        Assert.assertEquals("AND age GT 1", s2Where);
    }

    @Test
    public void joinConditionWithComplexObjectComparisonRightSide() throws SqlParseException {
        String query = String.format("select c.name.firstname,c.parents.father , h.name,h.words from %s/gotCharacters c " +
                "JOIN %s/gotCharacters h " +
                "on h.name = c.name.lastname  " +
                "where c.name.firstname='Daenerys'", TEST_INDEX_GAME_OF_THRONES, TEST_INDEX_GAME_OF_THRONES);
        JoinSelect joinSelect = parser.parseJoinSelect((SQLQueryExpr) queryToExpr(query));
        List<Condition> conditions = joinSelect.getConnectedConditions();
        Assert.assertNotNull(conditions);
        Assert.assertEquals(1, conditions.size());
        Assert.assertTrue("condition not exist: h.name = c.name.lastname", conditionExist(conditions, "h.name", "c.name.lastname", Condition.OPEAR.EQ));
    }

    @Test
    public void joinConditionWithComplexObjectComparisonLeftSide() throws SQLFeatureNotSupportedException, IOException, SqlParseException {
        String query = String.format("select c.name.firstname,c.parents.father , h.name,h.words from %s/gotCharacters c " +
                "JOIN %s/gotCharacters h " +
                "on c.name.lastname = h.name  " +
                "where c.name.firstname='Daenerys'", TEST_INDEX_GAME_OF_THRONES, TEST_INDEX_GAME_OF_THRONES);
        JoinSelect joinSelect = parser.parseJoinSelect((SQLQueryExpr) queryToExpr(query));
        List<Condition> conditions = joinSelect.getConnectedConditions();
        Assert.assertNotNull(conditions);
        Assert.assertEquals(1, conditions.size());
        Assert.assertTrue("condition not exist: c.name.lastname = h.name", conditionExist(conditions, "c.name.lastname", "h.name", Condition.OPEAR.EQ));
    }


    @Test
    public void limitHintsOnJoin() throws SqlParseException {
        String query = String.format("select /*! JOIN_TABLES_LIMIT(1000,null) */ c.name.firstname,c.parents.father , h.name,h.words from %s/gotCharacters c " +
                "use KEY (termsFilter) " +
                "JOIN %s/gotCharacters h " +
                "on c.name.lastname = h.name  " +
                "where c.name.firstname='Daenerys'", TEST_INDEX_GAME_OF_THRONES, TEST_INDEX_GAME_OF_THRONES);
        JoinSelect joinSelect = parser.parseJoinSelect((SQLQueryExpr) queryToExpr(query));
        List<Hint> hints = joinSelect.getHints();
        Assert.assertNotNull(hints);
        Assert.assertEquals("hints size was not 1", 1, hints.size());
        Hint hint = hints.get(0);
        Assert.assertEquals(HintType.JOIN_LIMIT, hint.getType());
        Object[] params = hint.getParams();
        Assert.assertNotNull(params);
        Assert.assertEquals("params size was not 2", 2, params.length);
        Assert.assertEquals(1000, params[0]);
        Assert.assertEquals(null, params[1]);
    }

    @Test
    public void hashTermsFilterHint() throws SqlParseException {
        String query = String.format("select /*! HASH_WITH_TERMS_FILTER*/ c.name.firstname,c.parents.father , h.name,h.words from %s/gotCharacters c " +
                "use KEY (termsFilter) " +
                "JOIN %s/gotCharacters h " +
                "on c.name.lastname = h.name  " +
                "where c.name.firstname='Daenerys'", TEST_INDEX_GAME_OF_THRONES, TEST_INDEX_GAME_OF_THRONES);
        JoinSelect joinSelect = parser.parseJoinSelect((SQLQueryExpr) queryToExpr(query));
        List<Hint> hints = joinSelect.getHints();
        Assert.assertNotNull(hints);
        Assert.assertEquals("hints size was not 1", 1, hints.size());
        Hint hint = hints.get(0);
        Assert.assertEquals(HintType.HASH_WITH_TERMS_FILTER, hint.getType());
    }

    @Test
    public void multipleHints() throws SqlParseException {
        String query = String.format("select /*! HASH_WITH_TERMS_FILTER*/ /*! JOIN_TABLES_LIMIT(1000,null) */ " +
                " /*! JOIN_TABLES_LIMIT(100,200) */ " +
                "c.name.firstname,c.parents.father , h.name,h.words from %s/gotCharacters c " +
                "use KEY (termsFilter) " +
                "JOIN %s/gotCharacters h " +
                "on c.name.lastname = h.name  " +
                "where c.name.firstname='Daenerys'", TEST_INDEX_GAME_OF_THRONES, TEST_INDEX_GAME_OF_THRONES);

        JoinSelect joinSelect = parser.parseJoinSelect((SQLQueryExpr) queryToExpr(query));
        List<Hint> hints = joinSelect.getHints();

        Assert.assertNotNull(hints);
        Assert.assertEquals("hints size was not 3", 3, hints.size());
        Hint firstHint = hints.get(0);
        Assert.assertEquals(HintType.HASH_WITH_TERMS_FILTER, firstHint.getType());
        Hint secondHint = hints.get(1);
        Assert.assertEquals(HintType.JOIN_LIMIT, secondHint.getType());
        Assert.assertEquals(1000, secondHint.getParams()[0]);
        Assert.assertEquals(null, secondHint.getParams()[1]);
        Hint thirdHint = hints.get(2);
        Assert.assertEquals(100, thirdHint.getParams()[0]);
        Assert.assertEquals(200, thirdHint.getParams()[1]);
        Assert.assertEquals(HintType.JOIN_LIMIT, thirdHint.getType());
    }

    @Test
    public void searchWithOdbcTimeFormatParse() throws SqlParseException {
        String query = String.format("SELECT insert_time FROM %s/odbc WHERE insert_time < {ts '2015-03-15 00:00:00.000'}", TEST_INDEX_ODBC);
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        LinkedList<Where> wheres = select.getWhere().getWheres();
        Assert.assertEquals(1, wheres.size());
        Condition condition = (Condition) wheres.get(0);
        Assert.assertEquals("{ts '2015-03-15 00:00:00.000'}", condition.getValue().toString());

    }

    @Test
    public void indexWithSpacesWithinBrackets() throws SqlParseException {
        String query = "SELECT insert_time FROM [Test Index] WHERE age > 3";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<From> fromList = select.getFrom();
        Assert.assertEquals(1, fromList.size());
        From from = fromList.get(0);
        Assert.assertEquals("Test Index", from.getIndex());
    }

    @Test
    public void indexWithSpacesWithTypeWithinBrackets() throws SqlParseException {
        String query = "SELECT insert_time FROM [Test Index]/type1 WHERE age > 3";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<From> fromList = select.getFrom();
        Assert.assertEquals(1, fromList.size());
        From from = fromList.get(0);
        Assert.assertEquals("Test Index", from.getIndex());
        Assert.assertEquals("type1", from.getType());
    }


    @Test
    public void fieldWithSpacesWithinBrackets() throws SqlParseException {
        String query = "SELECT insert_time FROM name/type1 WHERE [first name] = 'Name'";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<Where> where = select.getWhere().getWheres();
        Assert.assertEquals(1, where.size());
        Condition condition = (Condition) where.get(0);
        Assert.assertEquals("first name", condition.getName());
        Assert.assertEquals("Name", condition.getValue());
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
        boolean notPreservedOrder = from1.getIndex().equals("index2") && from1.getType().equals("type2")
                && from2.getIndex().equals("index1") && from2.getType().equals("type1");
        Assert.assertTrue(preservedOrder || notPreservedOrder);
    }

    @Test
    public void fieldWithATcharAtWhere() throws SqlParseException {
        String query = "SELECT * FROM index/type where @field = 6 ";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        LinkedList<Where> wheres = select.getWhere().getWheres();
        Assert.assertEquals(1, wheres.size());
        Condition condition = (Condition) wheres.get(0);
        Assert.assertEquals("@field", condition.getName());
    }

    @Test
    public void fieldWithATcharAtSelect() throws SqlParseException {
        String query = "SELECT @field FROM index/type where field2 = 6 ";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<Field> fields = select.getFields();
        Assert.assertEquals(1, fields.size());
        Field field = fields.get(0);
        Assert.assertEquals(field.getName(), "@field");
    }

    @Test
    public void fieldWithATcharAtSelectOnAgg() throws SqlParseException {
        String query = "SELECT max(@field) FROM index/type where field2 = 6 ";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<Field> fields = select.getFields();
        Assert.assertEquals(1, fields.size());
        Field field = fields.get(0);
        Assert.assertEquals("MAX(@field)", field.toString());
    }

    @Test
    public void fieldWithColonCharAtSelect() throws SqlParseException {
        String query = "SELECT a:b FROM index/type where field2 = 6 ";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<Field> fields = select.getFields();
        Assert.assertEquals(1, fields.size());
        Field field = fields.get(0);
        Assert.assertEquals(field.getName(), "a:b");
    }

    @Test
    public void fieldWithColonCharAtWhere() throws SqlParseException {
        String query = "SELECT * FROM index/type where a:b = 6 ";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        LinkedList<Where> wheres = select.getWhere().getWheres();
        Assert.assertEquals(1, wheres.size());
        Condition condition = (Condition) wheres.get(0);
        Assert.assertEquals("a:b", condition.getName());
    }

    @Test
    public void fieldIsNull() throws SqlParseException {
        String query = "SELECT * FROM index/type where a IS NOT NULL";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        LinkedList<Where> wheres = select.getWhere().getWheres();
        Assert.assertEquals(1, wheres.size());
        Condition condition = (Condition) wheres.get(0);
        Assert.assertEquals("a", condition.getName());
        Assert.assertNull(condition.getValue());
    }

    @Test
    public void innerQueryTest() throws SqlParseException {
        String query = String.format("select * from %s/dog where holdersName IN (select firstname from %s/account where firstname = 'eliran')", TEST_INDEX_DOG, TEST_INDEX_ACCOUNT);
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        Assert.assertTrue(select.containsSubQueries());
        Assert.assertEquals(1, select.getSubQueries().size());
    }

    @Test
    public void inTermsSubQueryTest() throws SqlParseException {
        String query = String.format("select * from %s/dog where holdersName = IN_TERMS (select firstname from %s/account where firstname = 'eliran')", TEST_INDEX_DOG, TEST_INDEX_ACCOUNT);
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        Assert.assertTrue(select.containsSubQueries());
        Assert.assertEquals(1, select.getSubQueries().size());
    }


    @Test
    public void innerQueryTestTwoQueries() throws SqlParseException {
        String query = String.format("select * from %s/dog where holdersName IN (select firstname from %s/account where firstname = 'eliran') and age IN (select name.ofHisName from %s/gotCharacters) ", TEST_INDEX_DOG, TEST_INDEX_ACCOUNT, TEST_INDEX_GAME_OF_THRONES);
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        Assert.assertTrue(select.containsSubQueries());
        Assert.assertEquals(2, select.getSubQueries().size());
    }

    @Test
    public void indexWithDotsAndHyphen() throws SqlParseException {
        String query = "select * from data-2015.08.22";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        Assert.assertEquals(1, select.getFrom().size());
        Assert.assertEquals("data-2015.08.22", select.getFrom().get(0).getIndex());
    }

    @Test
    public void indexWithSemiColons() throws SqlParseException {
        String query = "select * from some;index";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        Assert.assertEquals(1, select.getFrom().size());
        Assert.assertEquals("some;index", select.getFrom().get(0).getIndex());
    }

    @Test
    public void scriptFiledPlusLiteralTest() throws SqlParseException {
        String query = "SELECT field1 + 3 FROM index/type";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<Field> fields = select.getFields();
        Assert.assertEquals(1, fields.size());
        Field field = fields.get(0);
        Assert.assertTrue(field instanceof MethodField);
        MethodField scriptMethod = (MethodField) field;
        Assert.assertEquals("script", scriptMethod.getName().toLowerCase());
        Assert.assertEquals(2, scriptMethod.getParams().size());
        Assert.assertTrue(scriptMethod.getParams().get(1).toString().contains("doc['field1'].value + 3"));
    }

    @Test
    public void scriptFieldPlusFieldTest() throws SqlParseException {
        String query = "SELECT field1 + field2 FROM index/type";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<Field> fields = select.getFields();
        Assert.assertEquals(1, fields.size());
        Field field = fields.get(0);
        Assert.assertTrue(field instanceof MethodField);
        MethodField scriptMethod = (MethodField) field;
        Assert.assertEquals("script", scriptMethod.getName().toLowerCase());
        Assert.assertEquals(2, scriptMethod.getParams().size());
        Assert.assertTrue(scriptMethod.getParams().get(1).toString().contains("doc['field1'].value + doc['field2'].value"));
    }


    @Test
    public void scriptLiteralPlusLiteralTest() throws SqlParseException {
        String query = "SELECT 1 + 2  FROM index/type";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<Field> fields = select.getFields();
        Assert.assertEquals(1, fields.size());
        Field field = fields.get(0);
        Assert.assertTrue(field instanceof MethodField);
        MethodField scriptMethod = (MethodField) field;
        Assert.assertEquals("script", scriptMethod.getName().toLowerCase());
        Assert.assertEquals(2, scriptMethod.getParams().size());
        Assert.assertTrue(scriptMethod.getParams().get(1).toString().contains("1 + 2"));
    }


    @Test
    public void explicitScriptOnAggregation() throws SqlParseException {
        String query = "SELECT avg( script('add','doc[\\'field1\\'].value + doc[\\'field2\\'].value') ) FROM index/type";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<Field> fields = select.getFields();
        Assert.assertEquals(1, fields.size());
        Field field = fields.get(0);
        Assert.assertTrue(field instanceof MethodField);
        MethodField avgMethodField = (MethodField) field;
        Assert.assertEquals("avg", avgMethodField.getName().toLowerCase());
        Assert.assertEquals(1, avgMethodField.getParams().size());
        MethodField scriptMethod = (MethodField) avgMethodField.getParams().get(0).value;
        Assert.assertEquals("script", scriptMethod.getName().toLowerCase());
        Assert.assertEquals(2, scriptMethod.getParams().size());
        Assert.assertEquals("doc['field1'].value + doc['field2'].value", scriptMethod.getParams().get(1).toString());
    }

    @Test
    public void implicitScriptOnAggregation() throws SqlParseException {
        String query = "SELECT avg(field(field1) + field(field2)) FROM index/type";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<Field> fields = select.getFields();
        Assert.assertEquals(1, fields.size());
        Field field = fields.get(0);
        Assert.assertTrue(field instanceof MethodField);
        MethodField avgMethodField = (MethodField) field;
        Assert.assertEquals("avg", avgMethodField.getName().toLowerCase());
        Assert.assertEquals(1, avgMethodField.getParams().size());
        Assert.assertTrue(avgMethodField.getParams().get(0).value.toString().contains("doc['field1'].value"));
        Assert.assertTrue(avgMethodField.getParams().get(0).value.toString().contains("doc['field2'].value"));

    }

    @Test
    public void nestedFieldOnWhereNoPathSimpleField() throws SqlParseException {
        String query = "select * from myIndex where nested(message.name) = 'hey'";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        Where where = select.getWhere().getWheres().get(0);
        Assert.assertTrue("where should be condition", where instanceof Condition);
        Condition condition = (Condition) where;
        Assert.assertTrue("condition should be nested", condition.isNested());
        Assert.assertEquals("message", condition.getNestedPath());
        Assert.assertEquals("message.name", condition.getName());
    }


    @Test
    public void nestedFieldOnWhereNoPathComplexField() throws SqlParseException {
        String query = "select * from myIndex where nested(message.moreNested.name) = 'hey'";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        Where where = select.getWhere().getWheres().get(0);
        Assert.assertTrue("where should be condition", where instanceof Condition);
        Condition condition = (Condition) where;
        Assert.assertTrue("condition should be nested", condition.isNested());
        Assert.assertEquals("message.moreNested", condition.getNestedPath());
        Assert.assertEquals("message.moreNested.name", condition.getName());
    }


    @Test
    public void aggFieldWithAliasTableAliasShouldBeRemoved() throws SqlParseException {
        String query = "select count(t.*) as counts,sum(t.size) from xxx/locs as t group by t.kk";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<Field> fields = select.getFields();
        Assert.assertTrue(fields.size() == 2);
        Assert.assertEquals("COUNT(*)", fields.get(0).toString());
        Assert.assertEquals("SUM(size)", fields.get(1).toString());
        List<List<Field>> groups = select.getGroupBys();
        Assert.assertTrue(groups.size() == 1);
        Assert.assertTrue(groups.get(0).size() == 1);
        Assert.assertEquals("kk", groups.get(0).get(0).getName());
    }

    @Test
    public void nestedFieldOnWhereGivenPath() throws SqlParseException {
        String query = "select * from myIndex where nested(message.name,message) = 'hey'";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        Where where = select.getWhere().getWheres().get(0);
        Assert.assertTrue("where should be condition", where instanceof Condition);
        Condition condition = (Condition) where;
        Assert.assertTrue("condition should be nested", condition.isNested());
        Assert.assertEquals("message", condition.getNestedPath());
        Assert.assertEquals("message.name", condition.getName());
    }

    @Test
    public void nestedFieldOnGroupByNoPath() throws SqlParseException {
        String query = "select * from myIndex group by nested(message.name)";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        Field field = select.getGroupBys().get(0).get(0);
        Assert.assertTrue("condition should be nested", field.isNested());
        Assert.assertEquals("message", field.getNestedPath());
        Assert.assertEquals("message.name", field.getName());
    }

    @Test
    public void nestedFieldOnGroupByWithPath() throws SqlParseException {
        String query = "select * from myIndex group by nested(message.name,message)";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        Field field = select.getGroupBys().get(0).get(0);
        Assert.assertTrue("condition should be nested", field.isNested());
        Assert.assertEquals("message", field.getNestedPath());
        Assert.assertEquals("message.name", field.getName());
    }

    @Test
    public void filterAggTestNoAlias() throws SqlParseException {
        String query = "select * from myIndex group by a , filter(  a > 3 AND b='3' )";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<List<Field>> groupBys = select.getGroupBys();
        Assert.assertEquals(1, groupBys.size());
        Field aAgg = groupBys.get(0).get(0);
        Assert.assertEquals("a", aAgg.getName());
        Field field = groupBys.get(0).get(1);
        Assert.assertTrue("filter field should be method field", field instanceof MethodField);
        MethodField filterAgg = (MethodField) field;
        Assert.assertEquals("filter", filterAgg.getName());
        Map<String, Object> params = filterAgg.getParamsAsMap();
        Assert.assertEquals(2, params.size());
        Object alias = params.get("alias");
        Assert.assertEquals("filter(a > 3 AND b = '3')@FILTER", alias);

        Assert.assertTrue(params.get("where") instanceof Where);
        Where where = (Where) params.get("where");
        Assert.assertEquals(2, where.getWheres().size());
    }

    @Test
    public void filterAggTestWithAlias() throws SqlParseException {
        String query = "select * from myIndex group by a , filter(myFilter, a > 3 AND b='3' )";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<List<Field>> groupBys = select.getGroupBys();
        Assert.assertEquals(1, groupBys.size());
        Field aAgg = groupBys.get(0).get(0);
        Assert.assertEquals("a", aAgg.getName());
        Field field = groupBys.get(0).get(1);
        Assert.assertTrue("filter field should be method field", field instanceof MethodField);
        MethodField filterAgg = (MethodField) field;
        Assert.assertEquals("filter", filterAgg.getName());
        Map<String, Object> params = filterAgg.getParamsAsMap();
        Assert.assertEquals(2, params.size());
        Object alias = params.get("alias");
        Assert.assertEquals("myFilter@FILTER", alias);

        Assert.assertTrue(params.get("where") instanceof Where);
        Where where = (Where) params.get("where");
        Assert.assertEquals(2, where.getWheres().size());
    }


    @Test
    public void filterAggTestWithAliasAsString() throws SqlParseException {
        String query = "select * from myIndex group by a , filter('my filter', a > 3 AND b='3' )";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<List<Field>> groupBys = select.getGroupBys();
        Assert.assertEquals(1, groupBys.size());
        Field aAgg = groupBys.get(0).get(0);
        Assert.assertEquals("a", aAgg.getName());
        Field field = groupBys.get(0).get(1);
        Assert.assertTrue("filter field should be method field", field instanceof MethodField);
        MethodField filterAgg = (MethodField) field;
        Assert.assertEquals("filter", filterAgg.getName());
        Map<String, Object> params = filterAgg.getParamsAsMap();
        Assert.assertEquals(2, params.size());
        Object alias = params.get("alias");
        Assert.assertEquals("my filter@FILTER", alias);

        Assert.assertTrue(params.get("where") instanceof Where);
        Where where = (Where) params.get("where");
        Assert.assertEquals(2, where.getWheres().size());
    }

    @Test
    public void doubleOrderByTest() throws SqlParseException {
        String query = "select * from indexName order by a asc, b desc";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<Order> orderBys = select.getOrderBys();
        Assert.assertEquals(2, orderBys.size());
        Assert.assertEquals("a", orderBys.get(0).getName());
        Assert.assertEquals("ASC", orderBys.get(0).getType());

        Assert.assertEquals("b", orderBys.get(1).getName());
        Assert.assertEquals("DESC", orderBys.get(1).getType());
    }

    @Test
    public void parseJoinWithOneTableOrderByAttachToCorrectTable() throws SqlParseException {
        String query = String.format("select c.name.firstname , d.words from %s/gotCharacters c " +
                        "JOIN %s/gotCharacters d on d.name = c.house " +
                        "order by c.name.firstname"
                , TEST_INDEX_GAME_OF_THRONES, TEST_INDEX_GAME_OF_THRONES);

        JoinSelect joinSelect = parser.parseJoinSelect((SQLQueryExpr) queryToExpr(query));
        Assert.assertTrue("first table should be ordered", joinSelect.getFirstTable().isOrderdSelect());
        Assert.assertFalse("second table should not be ordered", joinSelect.getSecondTable().isOrderdSelect());

    }

    @Test
    public void parseJoinWithOneTableOrderByRemoveAlias() throws SqlParseException {
        String query = String.format("select c.name.firstname , d.words from %s/gotCharacters c " +
                        "JOIN %s/gotCharacters d on d.name = c.house " +
                        "order by c.name.firstname"
                , TEST_INDEX_GAME_OF_THRONES, TEST_INDEX_GAME_OF_THRONES);

        JoinSelect joinSelect = parser.parseJoinSelect((SQLQueryExpr) queryToExpr(query));
        List<Order> orderBys = joinSelect.getFirstTable().getOrderBys();
        Assert.assertEquals(1, orderBys.size());
        Order order = orderBys.get(0);
        Assert.assertEquals("name.firstname", order.getName());

    }

    @Test
    public void termsWithStringTest() throws SqlParseException {
        String query = "select * from x where y = IN_TERMS('a','b')";
        Select select = parser.parseSelect((SQLQueryExpr) queryToExpr(query));
        Condition condition = (Condition) select.getWhere().getWheres().get(0);
        Object[] values = (Object[]) condition.getValue();
        Assert.assertEquals("a", values[0]);
        Assert.assertEquals("b", values[1]);
    }

    @Test
    public void termWithStringTest() throws SqlParseException {
        String query = "select * from x where y = TERM('a')";
        Select select = parser.parseSelect((SQLQueryExpr) queryToExpr(query));
        Condition condition = (Condition) select.getWhere().getWheres().get(0);
        Object[] values = (Object[]) condition.getValue();
        Assert.assertEquals("a", values[0]);
    }

    @Test
    public void complexNestedTest() throws SqlParseException {
        String query = "select * from x where nested('y',y.b = 'a' and y.c  = 'd') ";
        Select select = parser.parseSelect((SQLQueryExpr) queryToExpr(query));
        Condition condition = (Condition) select.getWhere().getWheres().get(0);
        Assert.assertEquals(Condition.OPEAR.NESTED_COMPLEX, condition.getOpear());
        Assert.assertEquals("y", condition.getName());
        Assert.assertTrue(condition.getValue() instanceof Where);
        Where where = (Where) condition.getValue();
        Assert.assertEquals(2, where.getWheres().size());
    }

    @Test
    public void scriptOnFilterNoParams() throws SqlParseException {
        String query = "select * from x where script('doc[\\'field\\'].date.hourOfDay == 3') ";
        Select select = parser.parseSelect((SQLQueryExpr) queryToExpr(query));
        Condition condition = (Condition) select.getWhere().getWheres().get(0);
        Assert.assertEquals(Condition.OPEAR.SCRIPT, condition.getOpear());
        Assert.assertEquals(null, condition.getName());
        Assert.assertTrue(condition.getValue() instanceof ScriptFilter);
        ScriptFilter scriptFilter = (ScriptFilter) condition.getValue();
        Assert.assertEquals("doc['field'].date.hourOfDay == 3", scriptFilter.getScript());
        Assert.assertFalse(scriptFilter.containsParameters());
    }

    @Test
    public void scriptOnFilterWithParams() throws SqlParseException {
        String query = "select * from x where script('doc[\\'field\\'].date.hourOfDay == x','x'=3) ";
        Select select = parser.parseSelect((SQLQueryExpr) queryToExpr(query));
        Condition condition = (Condition) select.getWhere().getWheres().get(0);
        Assert.assertEquals(Condition.OPEAR.SCRIPT, condition.getOpear());
        Assert.assertEquals(null, condition.getName());
        Assert.assertTrue(condition.getValue() instanceof ScriptFilter);
        ScriptFilter scriptFilter = (ScriptFilter) condition.getValue();
        Assert.assertEquals("doc['field'].date.hourOfDay == x", scriptFilter.getScript());
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
        Assert.assertEquals(Condition.class, where.getClass());
        Condition condition = (Condition) where;
        Assert.assertEquals("3", condition.getName());
    }

    @Test
    public void likeTestWithEscaped() throws SqlParseException {
        String query = "select * from x where name like '&UNDERSCOREhey_%&PERCENT'";
        Select select = parser.parseSelect((SQLQueryExpr) queryToExpr(query));
        BoolQueryBuilder explan = QueryMaker.explan(select.getWhere());
        String filterAsString = explan.toString();
        Assert.assertTrue(filterAsString.contains("_hey?*%"));
    }


    @Test
    public void complexNestedAndOtherQuery() throws SqlParseException {
        String query = "select * from x where nested('path',path.x=3,'{\"from\":0}',score_mode=max) and y=3";
        Select select = parser.parseSelect((SQLQueryExpr) queryToExpr(query));
        LinkedList<Where> wheres = select.getWhere().getWheres();
        Assert.assertEquals(2, wheres.size());
        Assert.assertEquals("nested condition on path:path inner_hits:{\"from\":0} score_mode:Max AND path NESTED_COMPLEX AND ( AND path.x EQ 3 ) ", wheres.get(0).toString());
        Assert.assertEquals("AND y EQ 3", wheres.get(1).toString());
    }


    @Test
    public void numberEqualConditionWithoutProperty() throws SqlParseException {
        SQLExpr sqlExpr = queryToExpr("select * from xxx/locs where 1 = 1");
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<Where> wheres = select.getWhere().getWheres();
        Assert.assertTrue(wheres.size() == 1);
        Condition condition = (Condition) wheres.get(0);
        Assert.assertTrue(condition.getValue() instanceof ScriptFilter);
        ScriptFilter sf = (ScriptFilter) condition.getValue();
        Assert.assertEquals(sf.getScript(), "1 == 1");
    }

    @Test
    public void numberGreatConditionWithoutProperty() throws SqlParseException {
        SQLExpr sqlExpr = queryToExpr("select * from xxx/locs where 1 > 1");
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<Where> wheres = select.getWhere().getWheres();
        Assert.assertTrue(wheres.size() == 1);
        Condition condition = (Condition) wheres.get(0);
        Assert.assertTrue(condition.getValue() instanceof ScriptFilter);
        ScriptFilter sf = (ScriptFilter) condition.getValue();
        Assert.assertEquals(sf.getScript(), "1 > 1");
    }

    @Test
    public void stringEqualConditionWithoutProperty() throws SqlParseException {
        SQLExpr sqlExpr = queryToExpr("select * from xxx/locs where 'a' = 'b'");
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<Where> wheres = select.getWhere().getWheres();
        Assert.assertTrue(wheres.size() == 1);
        Condition condition = (Condition) wheres.get(0);
        Assert.assertTrue(condition.getValue() instanceof ScriptFilter);
        ScriptFilter sf = (ScriptFilter) condition.getValue();
        Assert.assertEquals(sf.getScript(), "'a' == 'b'");
    }

    @Test
    public void propertyEqualCondition() throws SqlParseException {
        SQLExpr sqlExpr = queryToExpr("select * from xxx/locs where a = b");
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<Where> wheres = select.getWhere().getWheres();
        Assert.assertTrue(wheres.size() == 1);
        Condition condition = (Condition) wheres.get(0);
        Assert.assertTrue(condition.getValue() instanceof ScriptFilter);
        ScriptFilter sf = (ScriptFilter) condition.getValue();
        Assert.assertEquals(sf.getScript(), "doc['a'].value == doc['b'].value");
    }


    @Test
    public void propertyWithTableAliasEqualCondition() throws SqlParseException {
        SQLExpr sqlExpr = queryToExpr("select t.* from xxx/locs where t.a = t.b");
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<Where> wheres = select.getWhere().getWheres();
        Assert.assertTrue(wheres.size() == 1);
        Condition condition = (Condition) wheres.get(0);
        Assert.assertTrue(condition.getValue() instanceof ScriptFilter);
        ScriptFilter sf = (ScriptFilter) condition.getValue();
        Assert.assertEquals(sf.getScript(), "doc['a'].value == doc['b'].value");
    }

    @Test
    public void propertyGreatCondition() throws SqlParseException {
        SQLExpr sqlExpr = queryToExpr("select * from xxx/locs where a > b");
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<Where> wheres = select.getWhere().getWheres();
        Assert.assertTrue(wheres.size() == 1);
        Condition condition = (Condition) wheres.get(0);
        Assert.assertTrue(condition.getValue() instanceof ScriptFilter);
        ScriptFilter sf = (ScriptFilter) condition.getValue();
        Assert.assertEquals(sf.getScript(), "doc['a'].value > doc['b'].value");
    }

    @Test
    public void stringAndNumberEqualConditionWithoutProperty() throws SqlParseException {
        SQLExpr sqlExpr = queryToExpr("select * from xxx/locs where 'a' = 1");
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        List<Where> wheres = select.getWhere().getWheres();
        Assert.assertTrue(wheres.size() == 1);
        Condition condition = (Condition) wheres.get(0);
        Assert.assertTrue(condition.getValue() instanceof ScriptFilter);
        ScriptFilter sf = (ScriptFilter) condition.getValue();
        Assert.assertEquals(sf.getScript(), "'a' == 1");
    }


    @Test
    public void caseWhenTest() throws SqlParseException {
        String query = "Select k,\n" +
                "Case \n" +
                "When floor(testBase)>=90 then 'A'\n" +
                "When testBase = '80' then 'B'\n" +
                "Else 'E' end as testBaseLevel\n" +
                "from t";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        for (Field field : select.getFields()) {
            if (field instanceof MethodField) {
                MethodField methodField = (MethodField) field;
                String alias = (String) methodField.getParams().get(0).value;
                String scriptCode = (String) methodField.getParams().get(1).value;
                Assert.assertEquals(alias, "testBaseLevel");
                Matcher docValue = Pattern.compile("doc\\['testBase'\\].value").matcher(scriptCode);
                Matcher number = Pattern.compile(" (\\s+90) | (\\s+'80')").matcher(scriptCode);

                AtomicInteger docValueCounter = new AtomicInteger();

                while (docValue.find()) {
                    docValueCounter.incrementAndGet();
                }

                Assert.assertTrue(docValueCounter.get() == 2);
                Assert.assertTrue(number.groupCount() == 2);

            }
        }

    }

    @Test
    public void caseWhenTestWithFieldElseExpr() throws SqlParseException {
        String query = "Select k,\n" +
                "Case \n" +
                "When floor(testBase)>=90 then 'A'\n" +
                "When testBase = '80' then 'B'\n" +
                "Else testBase end as testBaseLevel\n" +
                "from t";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        for (Field field : select.getFields()) {
            if (field instanceof MethodField) {
                MethodField methodField = (MethodField) field;
                String alias = (String) methodField.getParams().get(0).value;
                String scriptCode = (String) methodField.getParams().get(1).value;
                Assert.assertEquals(alias, "testBaseLevel");
                Matcher docValue = Pattern.compile("doc\\['testBase'\\].value").matcher(scriptCode);
                Matcher number = Pattern.compile(" (\\s+90) | (\\s+'80')").matcher(scriptCode);

                AtomicInteger docValueCounter = new AtomicInteger();

                while (docValue.find()) {
                    docValueCounter.incrementAndGet();
                }

                Assert.assertTrue(docValueCounter.get() == 3);
                Assert.assertTrue(number.groupCount() == 2);

            }
        }

    }

    @Test
    public void caseWhenTestWithouhtElseExpr() throws SqlParseException {
        String query = "Select k,\n" +
                "Case \n" +
                "When floor(testBase)>=90 then 'A'\n" +
                "When testBase = '80' then 'B'\n" +
                "end as testBaseLevel\n" +
                "from t";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        for (Field field : select.getFields()) {
            if (field instanceof MethodField) {
                MethodField methodField = (MethodField) field;
                String alias = (String) methodField.getParams().get(0).value;
                String scriptCode = (String) methodField.getParams().get(1).value;
                Assert.assertEquals(alias, "testBaseLevel");

                Matcher docValue = Pattern.compile("\\{\\s+null\\s+}").matcher(scriptCode);

                AtomicInteger docValueCounter = new AtomicInteger();

                while (docValue.find()) {
                    docValueCounter.incrementAndGet();
                }

                Assert.assertTrue(docValueCounter.get() == 1);

            }
        }

    }

    @Test
    public void castToIntTest() throws Exception {
        String query = "select cast(age as int) from "+ TEST_INDEX_ACCOUNT + "/account limit 10";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        Field castField = select.getFields().get(0);
        Assert.assertTrue(castField instanceof MethodField);

        MethodField methodField = (MethodField) castField;
        Assert.assertEquals("script",castField.getName());

        String alias = (String) methodField.getParams().get(0).value;
        String scriptCode = (String) methodField.getParams().get(1).value;
        Assert.assertEquals("cast_age",alias);
        Assert.assertTrue(scriptCode.contains("doc['age'].value"));
        Assert.assertTrue(scriptCode.contains("Double.parseDouble(doc['age'].value.toString()).intValue()"));
    }

    @Test
    public void castToLongTest() throws Exception {
        String query = "select cast(insert_time as long) from "+ TEST_INDEX_ACCOUNT + " limit 10";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        Field castField = select.getFields().get(0);
        Assert.assertTrue(castField instanceof MethodField);

        MethodField methodField = (MethodField) castField;
        Assert.assertEquals("script",castField.getName());

        String alias = (String) methodField.getParams().get(0).value;
        String scriptCode = (String) methodField.getParams().get(1).value;
        Assert.assertEquals("cast_insert_time",alias);
        Assert.assertTrue(scriptCode.contains("doc['insert_time'].value"));
        Assert.assertTrue(scriptCode.contains("Double.parseDouble(doc['insert_time'].value.toString()).longValue()"));
    }

    @Test
    public void castToFloatTest() throws Exception {
        String query = "select cast(age as float) from "+ TEST_INDEX_ACCOUNT + " limit 10";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        Field castField = select.getFields().get(0);
        Assert.assertTrue(castField instanceof MethodField);

        MethodField methodField = (MethodField) castField;
        Assert.assertEquals("script",castField.getName());

        String alias = (String) methodField.getParams().get(0).value;
        String scriptCode = (String) methodField.getParams().get(1).value;
        Assert.assertEquals("cast_age",alias);
        Assert.assertTrue(scriptCode.contains("doc['age'].value"));
        Assert.assertTrue(scriptCode.contains("Double.parseDouble(doc['age'].value.toString()).floatValue()"));
    }

    @Test
    public void castToDoubleTest() throws Exception {
        String query = "select cast(age as double) from "+ TEST_INDEX_ACCOUNT + "/account limit 10";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        Field castField = select.getFields().get(0);
        Assert.assertTrue(castField instanceof MethodField);

        MethodField methodField = (MethodField) castField;
        Assert.assertEquals("script",castField.getName());

        String alias = (String) methodField.getParams().get(0).value;
        String scriptCode = (String) methodField.getParams().get(1).value;
        Assert.assertEquals("cast_age",alias);
        Assert.assertTrue(scriptCode.contains("doc['age'].value"));
        Assert.assertTrue(scriptCode.contains("Double.parseDouble(doc['age'].value.toString()).doubleValue()"));
    }

    @Test
    public void castToStringTest() throws Exception {
        String query = "select cast(age as string) from "+ TEST_INDEX_ACCOUNT + "/account limit 10";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        Field castField = select.getFields().get(0);
        Assert.assertTrue(castField instanceof MethodField);

        MethodField methodField = (MethodField) castField;
        Assert.assertEquals("script",castField.getName());

        String alias = (String) methodField.getParams().get(0).value;
        String scriptCode = (String) methodField.getParams().get(1).value;
        Assert.assertEquals("cast_age",alias);
        Assert.assertTrue(scriptCode.contains("doc['age'].value.toString()"));
    }

    @Test
    public void castToDateTimeTest() throws Exception {
        String query = "select cast(age as datetime) from "+ TEST_INDEX_ACCOUNT + "/account limit 10";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        Field castField = select.getFields().get(0);
        Assert.assertTrue(castField instanceof MethodField);

        MethodField methodField = (MethodField) castField;
        Assert.assertEquals("script",castField.getName());

        String alias = (String) methodField.getParams().get(0).value;
        String scriptCode = (String) methodField.getParams().get(1).value;
        Assert.assertEquals("cast_age",alias);
        Assert.assertTrue(scriptCode.contains("doc['age'].value"));
        Assert.assertTrue(scriptCode.contains("new Date(Double.parseDouble(doc['age'].value.toString()).longValue())"));
    }

    @Test
    public void castToDoubleThenDivideTest() throws Exception {
        String query = "select cast(age as double)/2 from "+ TEST_INDEX_ACCOUNT + "/account limit 10";
        SQLExpr sqlExpr = queryToExpr(query);
        Select select = parser.parseSelect((SQLQueryExpr) sqlExpr);
        Field castField = select.getFields().get(0);
        Assert.assertTrue(castField instanceof MethodField);

        MethodField methodField = (MethodField) castField;
        Assert.assertEquals("script",castField.getName());

        String alias = (String) methodField.getParams().get(0).value;
        String scriptCode = (String) methodField.getParams().get(1).value;
        Assert.assertTrue(scriptCode.contains("doc['age'].value"));
        Assert.assertTrue(scriptCode.contains("Double.parseDouble(doc['age'].value.toString()).doubleValue()"));
        Assert.assertTrue(scriptCode.contains("/ 2"));
    }


    @Test
    public void multiSelectMinusOperationCheckIndices() throws SqlParseException {
        String query = "select pk from firstIndex minus  select pk from secondIndex ";
        MultiQuerySelect select = parser.parseMultiSelect((com.alibaba.druid.sql.ast.statement.SQLUnionQuery) ((SQLQueryExpr) queryToExpr(query)).getSubQuery().getQuery());
        Assert.assertEquals("firstIndex",select.getFirstSelect().getFrom().get(0).getIndex());
        Assert.assertEquals("secondIndex",select.getSecondSelect().getFrom().get(0).getIndex());
        Assert.assertEquals(SQLUnionOperator.MINUS,select.getOperation());
    }

    @Test
    public void multiSelectMinusWithAliasCheckAliases() throws SqlParseException {
        String query = "select pk as myId from firstIndex minus  select myId from secondIndex ";
        MultiQuerySelect select = parser.parseMultiSelect((com.alibaba.druid.sql.ast.statement.SQLUnionQuery) ((SQLQueryExpr) queryToExpr(query)).getSubQuery().getQuery());
        Assert.assertEquals("myId",select.getFirstSelect().getFields().get(0).getAlias());
        Assert.assertEquals("myId",select.getSecondSelect().getFields().get(0).getName());
        Assert.assertEquals(SQLUnionOperator.MINUS,select.getOperation());
    }
    @Test
    public void multiSelectMinusTestMinusHints() throws SqlParseException {
        String query = "select /*! MINUS_SCROLL_FETCH_AND_RESULT_LIMITS(1000,50,100)*/ /*! MINUS_USE_TERMS_OPTIMIZATION(true)*/ pk from firstIndex minus  select pk from secondIndex ";
        MultiQuerySelect select = parser.parseMultiSelect((com.alibaba.druid.sql.ast.statement.SQLUnionQuery) ((SQLQueryExpr) queryToExpr(query)).getSubQuery().getQuery());
        List<Hint> hints = select.getFirstSelect().getHints();
        Assert.assertEquals(2,hints.size());
        for(Hint hint : hints) {
            if (hint.getType() == HintType.MINUS_FETCH_AND_RESULT_LIMITS) {
                Object[] params = hint.getParams();
                Assert.assertEquals(1000,params[0]);
                Assert.assertEquals(50,params[1]);
                Assert.assertEquals(100,params[2]);
            }
            if(hint.getType() == HintType.MINUS_USE_TERMS_OPTIMIZATION){
                Assert.assertEquals(true,hint.getParams()[0]);
            }
        }
    }

    @Test
    public void multiSelectMinusScrollCheckDefaultsAllDefaults() throws SqlParseException {
        String query = "select /*! MINUS_SCROLL_FETCH_AND_RESULT_LIMITS*/ pk from firstIndex minus  select pk from secondIndex ";
        MultiQuerySelect select = parser.parseMultiSelect((com.alibaba.druid.sql.ast.statement.SQLUnionQuery) ((SQLQueryExpr) queryToExpr(query)).getSubQuery().getQuery());
        List<Hint> hints = select.getFirstSelect().getHints();
        Assert.assertEquals(1, hints.size());
        Hint hint = hints.get(0);
        Assert.assertEquals(HintType.MINUS_FETCH_AND_RESULT_LIMITS,hint.getType());
        Object[] params = hint.getParams();
        Assert.assertEquals(100000, params[0]);
        Assert.assertEquals(100000, params[1]);
        Assert.assertEquals(1000, params[2]);
    }

    @Test
    public void multiSelectMinusScrollCheckDefaultsOneDefault() throws SqlParseException {
        String query = "select /*! MINUS_SCROLL_FETCH_AND_RESULT_LIMITS(50,100)*/ pk from firstIndex minus  select pk from secondIndex ";
        MultiQuerySelect select = parser.parseMultiSelect((com.alibaba.druid.sql.ast.statement.SQLUnionQuery) ((SQLQueryExpr) queryToExpr(query)).getSubQuery().getQuery());
        List<Hint> hints = select.getFirstSelect().getHints();
        Assert.assertEquals(1, hints.size());
        Hint hint = hints.get(0);
        Assert.assertEquals(HintType.MINUS_FETCH_AND_RESULT_LIMITS,hint.getType());
        Object[] params = hint.getParams();
        Assert.assertEquals(50, params[0]);
        Assert.assertEquals(100, params[1]);
        Assert.assertEquals(1000, params[2]);
    }



    private SQLExpr queryToExpr(String query) {
        return new ElasticSqlExprParser(query).expr();
    }

    private boolean conditionExist(List<Condition> conditions, String from, String to, Condition.OPEAR opear) {
        String[] aliasAndField = to.split("\\.", 2);
        String toAlias = aliasAndField[0];
        String toField = aliasAndField[1];
        for (Condition condition : conditions) {
            if (condition.getOpear() != opear) continue;

            boolean fromIsEqual = condition.getName().equals(from);
            if (!fromIsEqual) continue;

            String[] valueAliasAndField = condition.getValue().toString().split("\\.", 2);
            boolean toFieldNameIsEqual = valueAliasAndField[1].equals(toField);
            boolean toAliasIsEqual = valueAliasAndField[0].equals(toAlias);
            boolean toIsEqual = toAliasIsEqual && toFieldNameIsEqual;

            if (toIsEqual) return true;
        }
        return false;
    }

}
