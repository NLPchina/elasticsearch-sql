/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class ParsedLongTerms extends ParsedTerms {

    @Override
    public String getType() {
        return LongTerms.NAME;
    }

    private static final ObjectParser<ParsedLongTerms, Void> PARSER = new ObjectParser<>(
            ParsedLongTerms.class.getSimpleName(),
            true,
            ParsedLongTerms::new
    );
    static {
        declareParsedTermsFields(PARSER, ParsedBucket::fromXContent);
    }

    public static ParsedLongTerms fromXContent(XContentParser parser, String name) throws IOException {
        ParsedLongTerms aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        return aggregation;
    }

    public static class ParsedBucket extends ParsedTerms.ParsedBucket {

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
            if (key != null) {
                return Long.toString(key);
            }
            return null;
        }

        public Number getKeyAsNumber() {
            return key;
        }

        @Override
        protected XContentBuilder keyToXContent(XContentBuilder builder) throws IOException {
            builder.field(CommonFields.KEY.getPreferredName(), key);
            if (super.getKeyAsString() != null) {
                builder.field(CommonFields.KEY_AS_STRING.getPreferredName(), getKeyAsString());
            }
            return builder;
        }

        static ParsedBucket fromXContent(XContentParser parser) throws IOException {
            return parseTermsBucketXContent(parser, ParsedBucket::new, (p, bucket) -> bucket.key = p.longValue());
        }
    }
}
