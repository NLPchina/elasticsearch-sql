package org.nlpcn.es4sql.parse;

import com.alibaba.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlLexer;
import com.alibaba.druid.sql.parser.Lexer;

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
}
