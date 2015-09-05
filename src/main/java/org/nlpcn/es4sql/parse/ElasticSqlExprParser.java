package org.nlpcn.es4sql.parse;

import com.alibaba.druid.sql.ast.SQLCommentHint;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlLexer;
import com.alibaba.druid.sql.parser.Lexer;
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

}
