package org.nlpcn.es4sql.parse;

import com.alibaba.druid.sql.ast.SQLCommentHint;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlLexer;
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
        return super.primary();
    }
}
