package org.nlpcn.es4sql.parse;

import com.alibaba.druid.sql.dialect.mysql.parser.MySqlLexer;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.Token;

import static com.alibaba.druid.sql.parser.CharTypes.isFirstIdentifierChar;
import static com.alibaba.druid.sql.parser.CharTypes.isIdentifierChar;
import static com.alibaba.druid.sql.parser.LayoutCharacters.EOI;

/**
 * Created by Eliran on 18/8/2015.
 */
public class ElasticLexer extends MySqlLexer {
    public ElasticLexer(String input) {
        super(input);
    }


    public ElasticLexer(char[] input, int inputLength, boolean skipComment) {
        super(input, inputLength, skipComment);
    }

    public void scanIdentifier() {
        final char first = ch;

        if (ch == '`') {

            mark = pos;
            bufPos = 1;
            char ch;
            for (;;) {
                ch = charAt(++pos);

                if (ch == '`') {
                    bufPos++;
                    ch = charAt(++pos);
                    break;
                } else if (ch == EOI) {
                    throw new ParserException("illegal identifier");
                }

                bufPos++;
                continue;
            }

            this.ch = charAt(pos);

            stringVal = subString(mark, bufPos);
            Token tok = keywods.getKeyword(stringVal);
            if (tok != null) {
                token = tok;
            } else {
                token = Token.IDENTIFIER;
            }
        } else {

            final boolean firstFlag = isFirstIdentifierChar(first);
            if (!firstFlag) {
                throw new ParserException("illegal identifier");
            }

            mark = pos;
            bufPos = 1;
            char ch;
            for (;;) {
                ch = charAt(++pos);

                if (!isElasticIdentifierChar(ch)) {
                    break;
                }

                bufPos++;
                continue;
            }

            this.ch = charAt(pos);

            stringVal = addSymbol();
            Token tok = keywods.getKeyword(stringVal);
            if (tok != null) {
                token = tok;
            } else {
                token = Token.IDENTIFIER;
            }
        }
    }


    private boolean isElasticIdentifierChar(char ch) {
        return ch == '*' || ch == '-'  || isIdentifierChar(ch);
    }
}
