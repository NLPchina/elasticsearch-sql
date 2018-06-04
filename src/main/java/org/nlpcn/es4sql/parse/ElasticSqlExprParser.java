package org.nlpcn.es4sql.parse;

import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlUnique;
import com.alibaba.druid.sql.dialect.mysql.ast.MysqlForeignKey;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSQLColumnDefinition;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.parser.*;
import com.alibaba.druid.util.JdbcConstants;

import java.util.List;

/**
 * Created by Eliran on 18/8/2015.
 */
public class ElasticSqlExprParser extends SQLExprParser {
    public ElasticSqlExprParser(Lexer lexer) {
        super(lexer);
        this.aggregateFunctions = AGGREGATE_FUNCTIONS;
    }

    public ElasticSqlExprParser(String sql) {
        this(new ElasticLexer(sql));
        this.lexer.nextToken();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public void parseHints(List hints) {
        while (lexer.token() == Token.HINT) {
            hints.add(new SQLCommentHint(lexer.stringVal()));
            lexer.nextToken();
        }
    }

    @Override
    protected SQLExpr methodRest(SQLExpr expr, boolean acceptLPAREN) {
        if (acceptLPAREN) {
            accept(Token.LPAREN);
        }

        if (expr instanceof SQLName || expr instanceof SQLDefaultExpr) {
            String methodName;

            SQLMethodInvokeExpr methodInvokeExpr;
            if (expr instanceof SQLPropertyExpr) {
                methodName = ((SQLPropertyExpr) expr).getName();
                methodInvokeExpr = new SQLMethodInvokeExpr(methodName);
                methodInvokeExpr.setOwner(((SQLPropertyExpr) expr).getOwner());
            } else {
                methodName = expr.toString();
                methodInvokeExpr = new SQLMethodInvokeExpr(methodName);
            }

            if (isAggreateFunction(methodName)) {
                SQLAggregateExpr aggregateExpr = parseAggregateExpr(methodName);

                return aggregateExpr;
            }

            if (lexer.token() != Token.RPAREN) {
                exprList(methodInvokeExpr.getParameters(), methodInvokeExpr);
            }

            accept(Token.RPAREN);

            return primaryRest(methodInvokeExpr);
        }

        throw new ParserException("not support token:" + lexer.token());
    }


    public SQLExpr primary() {

        if (lexer.token() == Token.LBRACE) {
            lexer.nextToken();
            boolean foundRBrace = false;
            if (lexer.stringVal().equals("ts")) {
                String current = lexer.stringVal();
                do {
                    if (current.equals(lexer.token().RBRACE.name())) {
                        foundRBrace = true;
                        break;
                    }
                    lexer.nextToken();
                    current = lexer.token().name();
                } while (!foundRBrace && !current.trim().equals(""));

                if (foundRBrace) {
                    SQLOdbcExpr sdle = new SQLOdbcExpr(lexer.stringVal());

                    accept(Token.RBRACE);
                    return sdle;
                } else {
                    throw new ParserException("Error. Unable to find closing RBRACE");
                }
            } else {
                throw new ParserException("Error. Unable to parse ODBC Literal Timestamp");
            }
        } else if (lexer.token() == Token.LBRACKET) {
            StringBuilder identifier = new StringBuilder();
            lexer.nextToken();
            String prefix = "";
            while (lexer.token() != Token.RBRACKET) {
                if (lexer.token() != Token.IDENTIFIER && lexer.token() != Token.INDEX && lexer.token() != Token.LITERAL_CHARS) {
                    throw new ParserException("All items between Brackets should be identifiers , got:" + lexer.token());
                }
                identifier.append(prefix);
                identifier.append(lexer.stringVal());
                prefix = " ";
                lexer.nextToken();
            }

            accept(Token.RBRACKET);
            return new SQLIdentifierExpr(identifier.toString());
        } else if (lexer.token() == Token.NOT) {
            lexer.nextToken();
            SQLExpr sqlExpr;
            if (lexer.token() == Token.EXISTS) {
                lexer.nextToken();
                accept(Token.LPAREN);
                sqlExpr = new SQLExistsExpr(createSelectParser().select(), true);
                accept(Token.RPAREN);
            } else if (lexer.token() == Token.LPAREN) {
                lexer.nextToken();

                SQLExpr notTarget = expr();

                accept(Token.RPAREN);

                sqlExpr = new SQLNotExpr(notTarget);

                return primaryRest(sqlExpr);
            } else {
                SQLExpr restExpr = relational();
                sqlExpr = new SQLNotExpr(restExpr);
            }
            return sqlExpr;
        }

        boolean parenWrapped = lexer.token() == Token.LPAREN;

        SQLExpr expr = primary2();

        // keep track of if the identifier is wrapped in parens
        if (parenWrapped && expr instanceof SQLIdentifierExpr) {
            expr = new SQLParensIdentifierExpr((SQLIdentifierExpr) expr);
        }

        return expr;
    }
    //zhongshu-comment 比父类的AGGREGATE_FUNCTIONS 多了 GROUP_CONCAT 这个function
    public static String[] AGGREGATE_FUNCTIONS = {"AVG", "COUNT", "GROUP_CONCAT", "MAX", "MIN", "STDDEV", "SUM"};


    public SQLExpr relationalRest(SQLExpr expr) {
        if (identifierEquals("REGEXP")) {
            lexer.nextToken();
            SQLExpr rightExp = equality();

            rightExp = relationalRest(rightExp);

            return new SQLBinaryOpExpr(expr, SQLBinaryOperator.RegExp, rightExp, JdbcConstants.MYSQL);
        }

        return super.relationalRest(expr);
    }

    public SQLExpr multiplicativeRest(SQLExpr expr) {
        if (lexer.token() == Token.IDENTIFIER && "MOD".equalsIgnoreCase(lexer.stringVal())) {
            lexer.nextToken();
            SQLExpr rightExp = primary();

            rightExp = relationalRest(rightExp);

            return new SQLBinaryOpExpr(expr, SQLBinaryOperator.Modulus, rightExp, JdbcConstants.MYSQL);
        }

        return super.multiplicativeRest(expr);
    }

    public SQLExpr notRationalRest(SQLExpr expr) {
        if (identifierEquals("REGEXP")) {
            lexer.nextToken();
            SQLExpr rightExp = primary();

            rightExp = relationalRest(rightExp);

            return new SQLBinaryOpExpr(expr, SQLBinaryOperator.NotRegExp, rightExp, JdbcConstants.MYSQL);
        }

        return super.notRationalRest(expr);
    }

    public SQLExpr primary2() {
        final Token tok = lexer.token();

        if (identifierEquals("outfile")) {
            lexer.nextToken();
            SQLExpr file = primary();
            SQLExpr expr = new MySqlOutFileExpr(file);

            return primaryRest(expr);

        }

        switch (tok) {
            case LITERAL_ALIAS:
                String aliasValue = lexer.stringVal();
                lexer.nextToken();
                return primaryRest(new SQLCharExpr(aliasValue));
            case VARIANT:
                SQLVariantRefExpr varRefExpr = new SQLVariantRefExpr(lexer.stringVal());
                lexer.nextToken();
                if (varRefExpr.getName().equalsIgnoreCase("@@global")) {
                    accept(Token.DOT);
                    varRefExpr = new SQLVariantRefExpr(lexer.stringVal(), true);
                    lexer.nextToken();
                } else if (varRefExpr.getName().equals("@") && lexer.token() == Token.LITERAL_CHARS) {
                    varRefExpr.setName("@'" + lexer.stringVal() + "'");
                    lexer.nextToken();
                } else if (varRefExpr.getName().equals("@@") && lexer.token() == Token.LITERAL_CHARS) {
                    varRefExpr.setName("@@'" + lexer.stringVal() + "'");
                    lexer.nextToken();
                }
                return primaryRest(varRefExpr);
            case VALUES:
                lexer.nextToken();
                if (lexer.token() != Token.LPAREN) {
                    throw new ParserException("syntax error, illegal values clause");
                }
                return this.methodRest(new SQLIdentifierExpr("VALUES"), true);
            case BINARY:
                lexer.nextToken();
                if (lexer.token() == Token.COMMA || lexer.token() == Token.SEMI || lexer.token() == Token.EOF) {
                    return new SQLIdentifierExpr("BINARY");
                } else {
                    SQLUnaryExpr binaryExpr = new SQLUnaryExpr(SQLUnaryOperator.BINARY, expr());
                    return primaryRest(binaryExpr);
                }
            case CACHE:
            case GROUP:
                lexer.nextToken();
                return primaryRest(new SQLIdentifierExpr(lexer.stringVal()));
            default:
                return super.primary();
        }

    }


    public final SQLExpr primaryRest(SQLExpr expr) {
        if (expr == null) {
            throw new IllegalArgumentException("expr");
        }

        if (lexer.token() == Token.LITERAL_CHARS) {
            if (expr instanceof SQLIdentifierExpr) {
                SQLIdentifierExpr identExpr = (SQLIdentifierExpr) expr;
                String ident = identExpr.getName();

                if (ident.equalsIgnoreCase("x")) {
                    String charValue = lexer.stringVal();
                    lexer.nextToken();
                    expr = new SQLHexExpr(charValue);

                    return primaryRest(expr);
                } else if (ident.equalsIgnoreCase("b")) {
                    String charValue = lexer.stringVal();
                    lexer.nextToken();
                    expr = new SQLBinaryExpr(charValue);

                    return primaryRest(expr);
                } else if (ident.startsWith("_")) {
                    String charValue = lexer.stringVal();
                    lexer.nextToken();

                    MySqlCharExpr mysqlCharExpr = new MySqlCharExpr(charValue);
                    mysqlCharExpr.setCharset(identExpr.getName());
                    if (identifierEquals("COLLATE")) {
                        lexer.nextToken();

                        String collate = lexer.stringVal();
                        mysqlCharExpr.setCollate(collate);
                        accept(Token.IDENTIFIER);
                    }

                    expr = mysqlCharExpr;

                    return primaryRest(expr);
                }
            } else if (expr instanceof SQLCharExpr) {
                SQLMethodInvokeExpr concat = new SQLMethodInvokeExpr("CONCAT");
                concat.addParameter(expr);
                do {
                    String chars = lexer.stringVal();
                    concat.addParameter(new SQLCharExpr(chars));
                    lexer.nextToken();
                } while (lexer.token() == Token.LITERAL_CHARS || lexer.token() == Token.LITERAL_ALIAS);
                expr = concat;
            }
        } else if (lexer.token() == Token.IDENTIFIER) {
            if (expr instanceof SQLHexExpr) {
                if ("USING".equalsIgnoreCase(lexer.stringVal())) {
                    lexer.nextToken();
                    if (lexer.token() != Token.IDENTIFIER) {
                        throw new ParserException("syntax error, illegal hex");
                    }
                    String charSet = lexer.stringVal();
                    lexer.nextToken();
                    expr.getAttributes().put("USING", charSet);

                    return primaryRest(expr);
                }
            } else if ("COLLATE".equalsIgnoreCase(lexer.stringVal())) {
                lexer.nextToken();

                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }

                if (lexer.token() != Token.IDENTIFIER) {
                    throw new ParserException("syntax error");
                }

                String collate = lexer.stringVal();
                lexer.nextToken();

                SQLBinaryOpExpr binaryExpr = new SQLBinaryOpExpr(expr, SQLBinaryOperator.COLLATE,
                        new SQLIdentifierExpr(collate), JdbcConstants.MYSQL);

                expr = binaryExpr;

                return primaryRest(expr);
            } else if (expr instanceof SQLVariantRefExpr) {
                if ("COLLATE".equalsIgnoreCase(lexer.stringVal())) {
                    lexer.nextToken();

                    if (lexer.token() != Token.IDENTIFIER) {
                        throw new ParserException("syntax error");
                    }

                    String collate = lexer.stringVal();
                    lexer.nextToken();

                    expr.putAttribute("COLLATE", collate);

                    return primaryRest(expr);
                }
            } else if (expr instanceof SQLIntegerExpr) {
                SQLIntegerExpr intExpr = (SQLIntegerExpr) expr;
                String binaryString = lexer.stringVal();
                if (intExpr.getNumber().intValue() == 0 && binaryString.startsWith("b")) {
                    lexer.nextToken();
                    expr = new SQLBinaryExpr(binaryString.substring(1));

                    return primaryRest(expr);
                }
            }
        }

        if (lexer.token() == Token.LPAREN && expr instanceof SQLIdentifierExpr) {
            SQLIdentifierExpr identExpr = (SQLIdentifierExpr) expr;
            String ident = identExpr.getName();

            if ("EXTRACT".equalsIgnoreCase(ident)) {
                lexer.nextToken();

                if (lexer.token() != Token.IDENTIFIER) {
                    throw new ParserException("syntax error");
                }

                String unitVal = lexer.stringVal();
                MySqlIntervalUnit unit = MySqlIntervalUnit.valueOf(unitVal.toUpperCase());
                lexer.nextToken();

                accept(Token.FROM);

                SQLExpr value = expr();

                MySqlExtractExpr extract = new MySqlExtractExpr();
                extract.setValue(value);
                extract.setUnit(unit);
                accept(Token.RPAREN);

                expr = extract;

                return primaryRest(expr);
            } else if ("SUBSTRING".equalsIgnoreCase(ident)) {
                lexer.nextToken();
                SQLMethodInvokeExpr methodInvokeExpr = new SQLMethodInvokeExpr(ident);
                for (; ; ) {
                    SQLExpr param = expr();
                    methodInvokeExpr.addParameter(param);

                    if (lexer.token() == Token.COMMA) {
                        lexer.nextToken();
                        continue;
                    } else if (lexer.token() == Token.FROM) {
                        lexer.nextToken();
                        SQLExpr from = expr();
                        methodInvokeExpr.addParameter(from);

                        if (lexer.token() == Token.FOR) {
                            lexer.nextToken();
                            SQLExpr forExpr = expr();
                            methodInvokeExpr.addParameter(forExpr);
                        }
                        break;
                    } else if (lexer.token() == Token.RPAREN) {
                        break;
                    } else {
                        throw new ParserException("syntax error");
                    }
                }

                accept(Token.RPAREN);
                expr = methodInvokeExpr;

                return primaryRest(expr);
            } else if ("TRIM".equalsIgnoreCase(ident)) {
                lexer.nextToken();
                SQLMethodInvokeExpr methodInvokeExpr = new SQLMethodInvokeExpr(ident);

                if (lexer.token() == Token.IDENTIFIER) {
                    String flagVal = lexer.stringVal();
                    if ("LEADING".equalsIgnoreCase(flagVal)) {
                        lexer.nextToken();
                        methodInvokeExpr.getAttributes().put("TRIM_TYPE", "LEADING");
                    } else if ("BOTH".equalsIgnoreCase(flagVal)) {
                        lexer.nextToken();
                        methodInvokeExpr.getAttributes().put("TRIM_TYPE", "BOTH");
                    } else if ("TRAILING".equalsIgnoreCase(flagVal)) {
                        lexer.nextToken();
                        methodInvokeExpr.putAttribute("TRIM_TYPE", "TRAILING");
                    }
                }

                SQLExpr param = expr();
                methodInvokeExpr.addParameter(param);

                if (lexer.token() == Token.FROM) {
                    lexer.nextToken();
                    SQLExpr from = expr();
                    methodInvokeExpr.putAttribute("FROM", from);
                }

                accept(Token.RPAREN);
                expr = methodInvokeExpr;

                return primaryRest(expr);
            } else if ("MATCH".equalsIgnoreCase(ident)) {
                lexer.nextToken();
                MySqlMatchAgainstExpr matchAgainstExpr = new MySqlMatchAgainstExpr();

                if (lexer.token() == Token.RPAREN) {
                    lexer.nextToken();
                } else {
                    exprList(matchAgainstExpr.getColumns(), matchAgainstExpr);
                    accept(Token.RPAREN);
                }

                acceptIdentifier("AGAINST");

                accept(Token.LPAREN);
                SQLExpr against = primary();
                matchAgainstExpr.setAgainst(against);

                if (lexer.token() == Token.IN) {
                    lexer.nextToken();
                    if (identifierEquals("NATURAL")) {
                        lexer.nextToken();
                        acceptIdentifier("LANGUAGE");
                        acceptIdentifier("MODE");
                        if (lexer.token() == Token.WITH) {
                            lexer.nextToken();
                            acceptIdentifier("QUERY");
                            acceptIdentifier("EXPANSION");
                            matchAgainstExpr.setSearchModifier(MySqlMatchAgainstExpr.SearchModifier.IN_NATURAL_LANGUAGE_MODE_WITH_QUERY_EXPANSION);
                        } else {
                            matchAgainstExpr.setSearchModifier(MySqlMatchAgainstExpr.SearchModifier.IN_NATURAL_LANGUAGE_MODE);
                        }
                    } else if (identifierEquals("BOOLEAN")) {
                        lexer.nextToken();
                        acceptIdentifier("MODE");
                        matchAgainstExpr.setSearchModifier(MySqlMatchAgainstExpr.SearchModifier.IN_BOOLEAN_MODE);
                    } else {
                        throw new ParserException("TODO");
                    }
                } else if (lexer.token() == Token.WITH) {
                    throw new ParserException("TODO");
                }

                accept(Token.RPAREN);

                expr = matchAgainstExpr;

                return primaryRest(expr);
            } else if ("CONVERT".equalsIgnoreCase(ident)) {
                lexer.nextToken();
                SQLMethodInvokeExpr methodInvokeExpr = new SQLMethodInvokeExpr(ident);

                if (lexer.token() != Token.RPAREN) {
                    exprList(methodInvokeExpr.getParameters(), methodInvokeExpr);
                }

                if (identifierEquals("USING")) {
                    lexer.nextToken();
                    if (lexer.token() != Token.IDENTIFIER) {
                        throw new ParserException("syntax error");
                    }
                    String charset = lexer.stringVal();
                    lexer.nextToken();
                    methodInvokeExpr.putAttribute("USING", charset);
                }

                accept(Token.RPAREN);

                expr = methodInvokeExpr;

                return primaryRest(expr);
            } else if ("POSITION".equalsIgnoreCase(ident)) {
                accept(Token.LPAREN);
                SQLExpr subStr = this.primary();
                accept(Token.IN);
                SQLExpr str = this.expr();
                accept(Token.RPAREN);

                SQLMethodInvokeExpr locate = new SQLMethodInvokeExpr("LOCATE");
                locate.addParameter(subStr);
                locate.addParameter(str);

                expr = locate;
                return primaryRest(expr);
            }
        }

        if (lexer.token() == Token.VARIANT && "@".equals(lexer.stringVal())) {
            lexer.nextToken();
            MySqlUserName userName = new MySqlUserName();
            if (expr instanceof SQLCharExpr) {
                userName.setUserName(((SQLCharExpr) expr).toString());
            } else {
                userName.setUserName(((SQLIdentifierExpr) expr).getName());
            }

            if (lexer.token() == Token.LITERAL_CHARS) {
                userName.setHost("'" + lexer.stringVal() + "'");
            } else {
                userName.setHost(lexer.stringVal());
            }
            lexer.nextToken();
            return userName;
        }

        //
        if (expr instanceof SQLMethodInvokeExpr && lexer.token() == Token.LBRACKET) {
            lexer.nextToken();
            expr = bracketRest(expr);
            return primaryRest(expr);
        }

        if (lexer.token() == Token.ERROR) {
            throw new ParserException("syntax error, token: " + lexer.token() + " " + lexer.stringVal() + ", pos : "
                    + lexer.pos());
        }

        return super.primaryRest(expr);
    }

    protected SQLExpr bracketRest(SQLExpr expr) {
        Number index;

        if (lexer.token() == Token.LITERAL_INT) {
            index = lexer.integerValue();
            lexer.nextToken();
        } else {
            throw new ParserException("error : " + lexer.stringVal());
        }

        if (expr instanceof SQLMethodInvokeExpr) {
            SQLMethodInvokeExpr methodInvokeExpr = (SQLMethodInvokeExpr) expr;
            methodInvokeExpr.getParameters().add(new SQLIntegerExpr(index));
        }
        lexer.nextToken();
        expr = primaryRest(expr);
        return expr;
    }

    public SQLSelectParser createSelectParser() {
        return new ElasticSqlSelectParser(this);
    }

    protected SQLExpr parseInterval() {
        accept(Token.INTERVAL);

        if (lexer.token() == Token.LPAREN) {
            lexer.nextToken();

            SQLMethodInvokeExpr methodInvokeExpr = new SQLMethodInvokeExpr("INTERVAL");
            if (lexer.token() != Token.RPAREN) {
                exprList(methodInvokeExpr.getParameters(), methodInvokeExpr);
            }

            accept(Token.RPAREN);

            return primaryRest(methodInvokeExpr);
        } else {
            SQLExpr value = expr();

            if (lexer.token() != Token.IDENTIFIER) {
                throw new ParserException("Syntax error");
            }

            String unit = lexer.stringVal();
            lexer.nextToken();

            MySqlIntervalExpr intervalExpr = new MySqlIntervalExpr();
            intervalExpr.setValue(value);
            intervalExpr.setUnit(MySqlIntervalUnit.valueOf(unit.toUpperCase()));

            return intervalExpr;
        }
    }

    public SQLColumnDefinition parseColumn() {
        MySqlSQLColumnDefinition column = new MySqlSQLColumnDefinition();
        column.setName(name());
        column.setDataType(parseDataType());

        return parseColumnRest(column);
    }

    public SQLColumnDefinition parseColumnRest(SQLColumnDefinition column) {
        if (lexer.token() == Token.ON) {
            lexer.nextToken();
            accept(Token.UPDATE);
            SQLExpr expr = this.expr();
            ((MySqlSQLColumnDefinition) column).setOnUpdate(expr);
        }

        if (identifierEquals("AUTO_INCREMENT")) {
            lexer.nextToken();
            if (column instanceof MySqlSQLColumnDefinition) {
                ((MySqlSQLColumnDefinition) column).setAutoIncrement(true);
            }
            return parseColumnRest(column);
        }

        if (identifierEquals("precision") && column.getDataType().getName().equalsIgnoreCase("double")) {
            lexer.nextToken();
        }

        if (identifierEquals("PARTITION")) {
            throw new ParserException("syntax error " + lexer.token() + " " + lexer.stringVal());
        }

        if (identifierEquals("STORAGE")) {
            lexer.nextToken();
            SQLExpr expr = expr();
            if (column instanceof MySqlSQLColumnDefinition) {
                ((MySqlSQLColumnDefinition) column).setStorage(expr);
            }
        }

        super.parseColumnRest(column);

        return column;
    }

    protected SQLDataType parseDataTypeRest(SQLDataType dataType) {
        super.parseDataTypeRest(dataType);

        if (identifierEquals("UNSIGNED")) {
            lexer.nextToken();
            dataType.getAttributes().put("UNSIGNED", true);
        }

        if (identifierEquals("ZEROFILL")) {
            lexer.nextToken();
            dataType.getAttributes().put("ZEROFILL", true);
        }

        return dataType;
    }

    public SQLExpr orRest(SQLExpr expr) {

        for (; ; ) {
            if (lexer.token() == Token.OR || lexer.token() == Token.BARBAR) {
                lexer.nextToken();
                SQLExpr rightExp = and();

                expr = new SQLBinaryOpExpr(expr, SQLBinaryOperator.BooleanOr, rightExp, JdbcConstants.MYSQL);
            } else if (lexer.token() == Token.XOR) {
                lexer.nextToken();
                SQLExpr rightExp = and();

                expr = new SQLBinaryOpExpr(expr, SQLBinaryOperator.BooleanXor, rightExp, JdbcConstants.MYSQL);
            } else {
                break;
            }
        }

        return expr;
    }

    public SQLExpr additiveRest(SQLExpr expr) {
        if (lexer.token() == Token.PLUS) {
            lexer.nextToken();
            SQLExpr rightExp = multiplicative();

            expr = new SQLBinaryOpExpr(expr, SQLBinaryOperator.Add, rightExp, JdbcConstants.MYSQL);
            expr = additiveRest(expr);
        } else if (lexer.token() == Token.SUB) {
            lexer.nextToken();
            SQLExpr rightExp = multiplicative();

            expr = new SQLBinaryOpExpr(expr, SQLBinaryOperator.Subtract, rightExp, JdbcConstants.MYSQL);
            expr = additiveRest(expr);
        }

        return expr;
    }

    public SQLAssignItem parseAssignItem() {
        SQLAssignItem item = new SQLAssignItem();

        SQLExpr var = primary();

        String ident = null;
        if (var instanceof SQLIdentifierExpr) {
            ident = ((SQLIdentifierExpr) var).getName();

            if ("GLOBAL".equalsIgnoreCase(ident)) {
                ident = lexer.stringVal();
                lexer.nextToken();
                var = new SQLVariantRefExpr(ident, true);
            } else if ("SESSION".equalsIgnoreCase(ident)) {
                ident = lexer.stringVal();
                lexer.nextToken();
                var = new SQLVariantRefExpr(ident, false);
            } else {
                var = new SQLVariantRefExpr(ident);
            }
        }

        if ("NAMES".equalsIgnoreCase(ident)) {
            // skip
        } else if ("CHARACTER".equalsIgnoreCase(ident)) {
            var = new SQLIdentifierExpr("CHARACTER SET");
            accept(Token.SET);
            if (lexer.token() == Token.EQ) {
                lexer.nextToken();
            }
        } else {
            if (lexer.token() == Token.COLONEQ) {
                lexer.nextToken();
            } else {
                accept(Token.EQ);
            }
        }

        item.setValue(this.expr());

        item.setTarget(var);
        return item;
    }

    public SQLName nameRest(SQLName name) {
        if (lexer.token() == Token.VARIANT && "@".equals(lexer.stringVal())) {
            lexer.nextToken();
            MySqlUserName userName = new MySqlUserName();
            userName.setUserName(((SQLIdentifierExpr) name).getName());

            if (lexer.token() == Token.LITERAL_CHARS) {
                userName.setHost("'" + lexer.stringVal() + "'");
            } else {
                userName.setHost(lexer.stringVal());
            }
            lexer.nextToken();
            return userName;
        }
        return super.nameRest(name);
    }

    public MySqlSelectQueryBlock.Limit parseLimit() {
        if (lexer.token() == Token.LIMIT) {
            lexer.nextToken();

            MySqlSelectQueryBlock.Limit limit = new MySqlSelectQueryBlock.Limit();

            SQLExpr temp = this.expr();
            if (lexer.token() == (Token.COMMA)) {
                limit.setOffset(temp);
                lexer.nextToken();
                limit.setRowCount(this.expr());
            } else if (identifierEquals("OFFSET")) {
                limit.setRowCount(temp);
                lexer.nextToken();
                limit.setOffset(this.expr());
            } else {
                limit.setRowCount(temp);
            }
            return limit;
        }

        return null;
    }

    @Override
    public MySqlPrimaryKey parsePrimaryKey() {
        accept(Token.PRIMARY);
        accept(Token.KEY);

        MySqlPrimaryKey primaryKey = new MySqlPrimaryKey();

        if (identifierEquals("USING")) {
            lexer.nextToken();
            primaryKey.setIndexType(lexer.stringVal());
            lexer.nextToken();
        }

        accept(Token.LPAREN);
        for (; ; ) {
            primaryKey.getColumns().add(this.expr());
            if (!(lexer.token() == (Token.COMMA))) {
                break;
            } else {
                lexer.nextToken();
            }
        }
        accept(Token.RPAREN);

        return primaryKey;
    }

    public MySqlUnique parseUnique() {
        accept(Token.UNIQUE);

        if (lexer.token() == Token.KEY) {
            lexer.nextToken();
        }

        if (lexer.token() == Token.INDEX) {
            lexer.nextToken();
        }

        MySqlUnique unique = new MySqlUnique();

        if (lexer.token() != Token.LPAREN) {
            SQLName indexName = name();
            unique.setIndexName(indexName);
        }

        accept(Token.LPAREN);
        for (; ; ) {
            unique.getColumns().add(this.expr());
            if (!(lexer.token() == (Token.COMMA))) {
                break;
            } else {
                lexer.nextToken();
            }
        }
        accept(Token.RPAREN);

        if (identifierEquals("USING")) {
            lexer.nextToken();
            unique.setIndexType(lexer.stringVal());
            lexer.nextToken();
        }

        return unique;
    }

    public MysqlForeignKey parseForeignKey() {
        accept(Token.FOREIGN);
        accept(Token.KEY);

        MysqlForeignKey fk = new MysqlForeignKey();

        if (lexer.token() != Token.LPAREN) {
            SQLName indexName = name();
            fk.setIndexName(indexName);
        }

        accept(Token.LPAREN);
        this.names(fk.getReferencingColumns());
        accept(Token.RPAREN);

        accept(Token.REFERENCES);

        fk.setReferencedTableName(this.name());

        accept(Token.LPAREN);
        this.names(fk.getReferencedColumns());
        accept(Token.RPAREN);

        if (identifierEquals("MATCH")) {
            if (identifierEquals("FULL")) {
                fk.setReferenceMatch(MysqlForeignKey.Match.FULL);
            } else if (identifierEquals("PARTIAL")) {
                fk.setReferenceMatch(MysqlForeignKey.Match.PARTIAL);
            } else if (identifierEquals("SIMPLE")) {
                fk.setReferenceMatch(MysqlForeignKey.Match.SIMPLE);
            }
        }

        if (lexer.token() == Token.ON) {
            lexer.nextToken();
            if (lexer.token() == Token.DELETE) {
                fk.setReferenceOn(MysqlForeignKey.On.DELETE);
            } else if (lexer.token() == Token.UPDATE) {
                fk.setReferenceOn(MysqlForeignKey.On.UPDATE);
            } else {
                throw new ParserException("syntax error, expect DELETE or UPDATE, actual " + lexer.token() + " "
                        + lexer.stringVal());
            }
            lexer.nextToken();

            if (lexer.token() == Token.RESTRICT) {
                fk.setReferenceOption(MysqlForeignKey.Option.RESTRICT);
            } else if (identifierEquals("CASCADE")) {
                fk.setReferenceOption(MysqlForeignKey.Option.CASCADE);
            } else if (lexer.token() == Token.SET) {
                accept(Token.NULL);
                fk.setReferenceOption(MysqlForeignKey.Option.SET_NULL);
            } else if (identifierEquals("ON")) {
                lexer.nextToken();
                if (identifierEquals("ACTION")) {
                    fk.setReferenceOption(MysqlForeignKey.Option.NO_ACTION);
                } else {
                    throw new ParserException("syntax error, expect ACTION, actual " + lexer.token() + " "
                            + lexer.stringVal());
                }
            }
            lexer.nextToken();
        }
        return fk;
    }

    protected SQLAggregateExpr parseAggregateExprRest(SQLAggregateExpr aggregateExpr) {
        if (lexer.token() == Token.ORDER) {
            SQLOrderBy orderBy = this.parseOrderBy();
            aggregateExpr.putAttribute("ORDER BY", orderBy);
        }
        if (identifierEquals("SEPARATOR")) {
            lexer.nextToken();

            SQLExpr seperator = this.primary();

            aggregateExpr.putAttribute("SEPARATOR", seperator);
        }
        return aggregateExpr;
    }

    public MySqlSelectGroupByExpr parseSelectGroupByItem() {
        MySqlSelectGroupByExpr item = new MySqlSelectGroupByExpr();

        item.setExpr(expr());

        if (lexer.token() == Token.ASC) {
            lexer.nextToken();
            item.setType(SQLOrderingSpecification.ASC);
        } else if (lexer.token() == Token.DESC) {
            lexer.nextToken();
            item.setType(SQLOrderingSpecification.DESC);
        }

        return item;
    }

}
