package org.nlpcn.es4sql.parse;

import com.alibaba.druid.sql.ast.SQLCommentHint;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlLexer;
import com.alibaba.druid.sql.parser.Lexer;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.Token;

import java.util.ArrayList;
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
            List<String> identifiers = new ArrayList<>();
            lexer.nextToken();
            while(lexer.token()!=Token.RBRACKET){
                if(lexer.token() != Token.IDENTIFIER && lexer.token()!=Token.INDEX){
                    throw new ParserException("All items between Brackets should be identifiers , got:" +lexer.token());
                }
                identifiers.add(lexer.stringVal());
                lexer.nextToken();
            }
            String identifier = String.join(" ", identifiers);
            accept(Token.RBRACKET);
            return new SQLIdentifierExpr(identifier);
        }
        return super.primary();
    }
}
