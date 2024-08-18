/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class ParsedSignificantLongTerms extends ParsedSignificantTerms {

    @Override
    public String getType() {
        return SignificantLongTerms.NAME;
    }

    private static final ObjectParser<ParsedSignificantLongTerms, Void> PARSER = new ObjectParser<>(
            ParsedSignificantLongTerms.class.getSimpleName(),
            true,
            ParsedSignificantLongTerms::new
    );
    static {
        declareParsedSignificantTermsFields(PARSER, ParsedBucket::fromXContent);
    }

    public static ParsedSignificantLongTerms fromXContent(XContentParser parser, String name) throws IOException {
        return parseSignificantTermsXContent(() -> PARSER.parse(parser, null), name);
    }

    public static class ParsedBucket extends ParsedSignificantTerms.ParsedBucket {

        private Long key;

        @Override
        public Object getKey() {
            return key;
        }

        @Override
        public String getKeyAsString() {
            String keyAsString = super.getKeyAsString();
            if (keyAsString != null) {
                return keyAsString;
            }
            return Long.toString(key);
        }

        @Override
        protected XContentBuilder keyToXContent(XContentBuilder builder) throws IOException {
            builder.field(Aggregation.CommonFields.KEY.getPreferredName(), key);
            if (super.getKeyAsString() != null) {
                builder.field(Aggregation.CommonFields.KEY_AS_STRING.getPreferredName(), getKeyAsString());
            }
            return builder;
        }

        static ParsedBucket fromXContent(XContentParser parser) throws IOException {
            return parseSignificantTermsBucketXContent(parser, new ParsedBucket(), (p, bucket) -> bucket.key = p.longValue());
        }
    }
}
