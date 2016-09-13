package org.nlpcn.es4sql.parse;

import com.alibaba.druid.sql.ast.SQLCommentHint;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLExistsExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLNotExpr;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.alibaba.druid.sql.parser.Lexer;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.Token;

import java.util.List;

/**
 * Created by Eliran on 18/8/2015.
 */
public class ElasticSqlExprParser extends MySqlExprParser {
    public ElasticSqlExprParser(Lexer lexer) {
        super(lexer);
    }

    public ElasticSqlExprParser(String sql) {
        this(new ElasticLexer(sql));
        this.lexer.nextToken();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void parseHints(List hints) {
        while (lexer.token() == Token.HINT) {
            hints.add(new SQLCommentHint(lexer.stringVal()));
            lexer.nextToken();
        }
    }

    @Override
    public SQLExpr primary() {

        if(lexer.token() == Token.LBRACE){
            lexer.nextToken();
            boolean foundRBrace = false;
            if(lexer.stringVal().equals("ts")){
                String current = lexer.stringVal();
                do {
                    if(current.equals(lexer.token().RBRACE.name())){
                        foundRBrace = true;
                        break;
                    }
                    lexer.nextToken();
                    current = lexer.token().name();
                }while(!foundRBrace && !current.trim().equals(""));

                if(foundRBrace){
                    SQLOdbcExpr sdle = new SQLOdbcExpr(lexer.stringVal());

                    accept(Token.RBRACE);
                    return sdle;
                }else{
                    throw new ParserException("Error. Unable to find closing RBRACE");
                }
            }else{
                throw new ParserException("Error. Unable to parse ODBC Literal Timestamp");
            }
        }
        else if(lexer.token() == Token.LBRACKET){
            StringBuilder identifier = new StringBuilder();
            lexer.nextToken();
            String prefix = "";
            while(lexer.token()!=Token.RBRACKET){
                if(lexer.token() != Token.IDENTIFIER && lexer.token()!=Token.INDEX && lexer.token()!=Token.LITERAL_CHARS){
                    throw new ParserException("All items between Brackets should be identifiers , got:" +lexer.token());
                }
                identifier.append(prefix);
                identifier.append(lexer.stringVal());
                prefix = " ";
                lexer.nextToken();
            }

            accept(Token.RBRACKET);
            return new SQLIdentifierExpr(identifier.toString());
        }
        else if (lexer.token() == Token.NOT) {
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

        SQLExpr expr = super.primary();

        // keep track of if the identifier is wrapped in parens
        if (parenWrapped && expr instanceof SQLIdentifierExpr) {
            expr = new SQLParensIdentifierExpr((SQLIdentifierExpr) expr);
        }

        return expr;
    }
}
