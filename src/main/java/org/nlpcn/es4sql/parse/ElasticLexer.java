package org.nlpcn.es4sql.parse;

import com.alibaba.druid.sql.dialect.mysql.parser.MySqlLexer;
import com.alibaba.druid.sql.parser.CharTypes;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SymbolTable;
import com.alibaba.druid.sql.parser.Token;

import static com.alibaba.druid.sql.parser.CharTypes.isFirstIdentifierChar;
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
        hash_lower = 0;
        hash = 0;

        final char first = ch;

        if ((ch == 'b' || ch == 'B' )
                && charAt(pos + 1) == '\'') {
            int i = 2;
            int mark = pos + 2;
            for (;;++i) {
                char ch = charAt(pos + i);
                if (ch == '0' || ch == '1') {
                    continue;
                } else if (ch == '\'') {
                    bufPos += i;
                    pos += (i + 1);
                    stringVal = subString(mark, i - 2);
                    this.ch = charAt(pos);
                    token = Token.BITS;
                    return;
                } else if (ch == EOI) {
                    throw new ParserException("illegal identifier. " + info());
                } else {
                    break;
                }
            }
        }

        if (ch == '`') {
            mark = pos;
            bufPos = 1;
            char ch;

            int startPos = pos + 1;
            int quoteIndex = text.indexOf('`', startPos);
            if (quoteIndex == -1) {
                throw new ParserException("illegal identifier. " + info());
            }

            hash_lower = 0xcbf29ce484222325L;
            hash = 0xcbf29ce484222325L;

            for (int i = startPos; i < quoteIndex; ++i) {
                ch = text.charAt(i);

                hash_lower ^= ((ch >= 'A' && ch <= 'Z') ? (ch + 32) : ch);
                hash_lower *= 0x100000001b3L;

                hash ^= ch;
                hash *= 0x100000001b3L;
            }

            stringVal = quoteTable.addSymbol(text, pos, quoteIndex + 1 - pos, hash);
            //stringVal = text.substring(mark, pos);
            pos = quoteIndex + 1;
            this.ch = charAt(pos);
            token = Token.IDENTIFIER;
        } else {
            final boolean firstFlag = isFirstIdentifierChar(first);
            if (!firstFlag) {
                throw new ParserException("illegal identifier. " + info());
            }

            hash_lower = 0xcbf29ce484222325L;
            hash = 0xcbf29ce484222325L;

            hash_lower ^= ((ch >= 'A' && ch <= 'Z') ? (ch + 32) : ch);
            hash_lower *= 0x100000001b3L;

            hash ^= ch;
            hash *= 0x100000001b3L;

            mark = pos;
            bufPos = 1;
            char ch = '\0';
            for (;;) {
                ch = charAt(++pos);

                if (!isElasticIdentifierChar(ch)) {
                    break;
                }

                bufPos++;

                hash_lower ^= ((ch >= 'A' && ch <= 'Z') ? (ch + 32) : ch);
                hash_lower *= 0x100000001b3L;

                hash ^= ch;
                hash *= 0x100000001b3L;

                continue;
            }

            this.ch = charAt(pos);

            if (bufPos == 1) {
                token = Token.IDENTIFIER;
                stringVal = CharTypes.valueOf(first);
                if (stringVal == null) {
                    stringVal = Character.toString(first);
                }
                return;
            }

            Token tok = keywods.getKeyword(hash_lower);
            if (tok != null) {
                token = tok;
                if (token == Token.IDENTIFIER) {
                    stringVal = SymbolTable.global.addSymbol(text, mark, bufPos, hash);
                } else {
                    stringVal = null;
                }
            } else {
                token = Token.IDENTIFIER;
                stringVal = SymbolTable.global.addSymbol(text, mark, bufPos, hash);
            }

        }
    }


    private boolean isElasticIdentifierChar(char ch) {
        return ch == '*' || ch == ':' || ch == '-'  || ch == '.' || ch == ';' || isIdentifierChar(ch);
    }
}
