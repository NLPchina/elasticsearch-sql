/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.suggest;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Top level suggest result, containing the result for each suggestion.
 */
public final class ParsedSuggest {

    /**
     * this parsing method assumes that the leading "suggest" field name has already been parsed by the caller
     */
    public static Suggest fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        List<Suggest.Suggestion<? extends Entry<? extends Option>>> suggestions = new ArrayList<>();
        while ((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
            String currentField = parser.currentName();
            ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.nextToken(), parser);
            Suggest.Suggestion<? extends Entry<? extends Option>> suggestion = ParsedSuggestion.fromXContent(parser);
            if (suggestion != null) {
                suggestions.add(suggestion);
            } else {
                throw new ParsingException(
                        parser.getTokenLocation(),
                        String.format(Locale.ROOT, "Could not parse suggestion keyed as [%s]", currentField)
                );
            }
        }
        return new Suggest(suggestions);
    }

    /**
     * The suggestion responses corresponding with the suggestions in the request.
     */
    @SuppressWarnings("rawtypes")
    public abstract static class ParsedSuggestion {

        @SuppressWarnings("unchecked")
        public static Suggest.Suggestion<? extends Entry<? extends Option>> fromXContent(XContentParser parser) throws IOException {
            ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
            SetOnce<Suggest.Suggestion> suggestion = new SetOnce<>();
            XContentParserUtils.parseTypedKeysObject(parser, Aggregation.TYPED_KEYS_DELIMITER, Suggest.Suggestion.class, suggestion::set);
            return suggestion.get();
        }

        protected static <E extends Suggest.Suggestion.Entry<?>> void parseEntries(
                XContentParser parser,
                Suggest.Suggestion<E> suggestion,
                CheckedFunction<XContentParser, E, IOException> entryParser
        ) throws IOException {
            ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
            while ((parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                suggestion.addTerm(entryParser.apply(parser));
            }
        }
    }
}
