/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ObjectParser.ValueType;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureFieldName;
import static org.elasticsearch.core.RefCounted.ALWAYS_REFERENCED;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * A single search hit.
 *
 * @see SearchHits
 */
public final class ParsedSearchHit {

    // All fields on the root level of the parsed SearhHit are interpreted as metadata fields
    // public because we use it in a completion suggestion option
    @SuppressWarnings("unchecked")
    public static final ObjectParser.UnknownFieldConsumer<Map<String, Object>> unknownMetaFieldConsumer = (map, fieldName, fieldValue) -> {
        Map<String, DocumentField> fieldMap = (Map<String, DocumentField>) map.computeIfAbsent(
                SearchHit.METADATA_FIELDS,
                v -> new HashMap<String, DocumentField>()
        );
        if (fieldName.equals(IgnoredFieldMapper.NAME)) {
            fieldMap.put(fieldName, new DocumentField(fieldName, (List<Object>) fieldValue));
        } else {
            fieldMap.put(fieldName, new DocumentField(fieldName, Collections.singletonList(fieldValue)));
        }
    };

    /**
     * This parser outputs a temporary map of the objects needed to create the
     * SearchHit instead of directly creating the SearchHit. The reason for this
     * is that this way we can reuse the parser when parsing xContent from
     * {@link org.elasticsearch.search.suggest.completion.CompletionSuggestion.Entry.Option} which unfortunately inlines
     * the output of
     * {@link #toInnerXContent(XContentBuilder, org.elasticsearch.xcontent.ToXContent.Params)}
     * of the included search hit. The output of the map is used to create the
     * actual SearchHit instance via {@link #createFromMap(Map)}
     */
    private static final ObjectParser<Map<String, Object>, Void> MAP_PARSER = new ObjectParser<>(
            "innerHitParser",
            unknownMetaFieldConsumer,
            HashMap::new
    );

    static {
        declareInnerHitsParseFields(MAP_PARSER);
    }

    public static SearchHit fromXContent(XContentParser parser) {
        return createFromMap(MAP_PARSER.apply(parser, null));
    }

    public static void declareInnerHitsParseFields(ObjectParser<Map<String, Object>, Void> parser) {
        parser.declareString((map, value) -> map.put(SearchHit.Fields._INDEX, value), new ParseField(SearchHit.Fields._INDEX));
        parser.declareString((map, value) -> map.put(SearchHit.Fields._ID, value), new ParseField(SearchHit.Fields._ID));
        parser.declareString((map, value) -> map.put(SearchHit.Fields._NODE, value), new ParseField(SearchHit.Fields._NODE));
        parser.declareField(
                (map, value) -> map.put(SearchHit.Fields._SCORE, value),
                ParsedSearchHit::parseScore,
                new ParseField(SearchHit.Fields._SCORE),
                ValueType.FLOAT_OR_NULL
        );
        parser.declareInt((map, value) -> map.put(SearchHit.Fields._RANK, value), new ParseField(SearchHit.Fields._RANK));

        parser.declareLong((map, value) -> map.put(SearchHit.Fields._VERSION, value), new ParseField(SearchHit.Fields._VERSION));
        parser.declareLong((map, value) -> map.put(SearchHit.Fields._SEQ_NO, value), new ParseField(SearchHit.Fields._SEQ_NO));
        parser.declareLong((map, value) -> map.put(SearchHit.Fields._PRIMARY_TERM, value), new ParseField(SearchHit.Fields._PRIMARY_TERM));
        parser.declareField(
                (map, value) -> map.put(SearchHit.Fields._SHARD, value),
                (p, c) -> ShardId.fromString(p.text()),
                new ParseField(SearchHit.Fields._SHARD),
                ValueType.STRING
        );
        parser.declareObject(
                (map, value) -> map.put(SourceFieldMapper.NAME, value),
                (p, c) -> parseSourceBytes(p),
                new ParseField(SourceFieldMapper.NAME)
        );
        parser.declareObject(
                (map, value) -> map.put(SearchHit.Fields.HIGHLIGHT, value),
                (p, c) -> parseHighlightFields(p),
                new ParseField(SearchHit.Fields.HIGHLIGHT)
        );
        parser.declareObject((map, value) -> {
            Map<String, DocumentField> fieldMap = get(SearchHit.Fields.FIELDS, map, new HashMap<String, DocumentField>());
            fieldMap.putAll(value);
            map.put(SearchHit.DOCUMENT_FIELDS, fieldMap);
        }, (p, c) -> parseFields(p), new ParseField(SearchHit.Fields.FIELDS));
        parser.declareObject(
                (map, value) -> map.put(SearchHit.Fields._EXPLANATION, value),
                (p, c) -> parseExplanation(p),
                new ParseField(SearchHit.Fields._EXPLANATION)
        );
        parser.declareObject(
                (map, value) -> map.put(SearchHit.NestedIdentity._NESTED, value),
                ParsedNestedIdentity::fromXContent,
                new ParseField(SearchHit.NestedIdentity._NESTED)
        );
        parser.declareObject(
                (map, value) -> map.put(SearchHit.Fields.INNER_HITS, value),
                (p, c) -> parseInnerHits(p),
                new ParseField(SearchHit.Fields.INNER_HITS)
        );

        parser.declareField((p, map, context) -> {
            XContentParser.Token token = p.currentToken();
            Map<String, Float> matchedQueries = new LinkedHashMap<>();
            if (token == XContentParser.Token.START_OBJECT) {
                String fieldName = null;
                while ((token = p.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        fieldName = p.currentName();
                    } else if (token.isValue()) {
                        matchedQueries.put(fieldName, p.floatValue());
                    }
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                while (p.nextToken() != XContentParser.Token.END_ARRAY) {
                    matchedQueries.put(p.text(), Float.NaN);
                }
            }
            map.put(SearchHit.Fields.MATCHED_QUERIES, matchedQueries);
        }, new ParseField(SearchHit.Fields.MATCHED_QUERIES), ObjectParser.ValueType.OBJECT_ARRAY);

        parser.declareField(
                (map, list) -> map.put(SearchHit.Fields.SORT, list),
                SearchSortValues::fromXContent,
                new ParseField(SearchHit.Fields.SORT),
                ValueType.OBJECT_ARRAY
        );
    }

    public static SearchHit createFromMap(Map<String, Object> values) {
        String id = get(SearchHit.Fields._ID, values, null);
        String index = get(SearchHit.Fields._INDEX, values, null);
        String clusterAlias = null;
        if (index != null) {
            int indexOf = index.indexOf(RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR);
            if (indexOf > 0) {
                clusterAlias = index.substring(0, indexOf);
                index = index.substring(indexOf + 1);
            }
        }
        ShardId shardId = get(SearchHit.Fields._SHARD, values, null);
        String nodeId = get(SearchHit.Fields._NODE, values, null);
        final SearchShardTarget shardTarget;
        if (shardId != null && nodeId != null) {
            assert shardId.getIndexName().equals(index);
            shardTarget = new SearchShardTarget(nodeId, shardId, clusterAlias);
            index = shardTarget.getIndex();
            clusterAlias = shardTarget.getClusterAlias();
        } else {
            shardTarget = null;
        }
        return new SearchHit(
                -1,
                get(SearchHit.Fields._SCORE, values, SearchHit.DEFAULT_SCORE),
                get(SearchHit.Fields._RANK, values, SearchHit.NO_RANK),
                id == null ? null : new Text(id),
                get(SearchHit.NestedIdentity._NESTED, values, null),
                get(SearchHit.Fields._VERSION, values, -1L),
                get(SearchHit.Fields._SEQ_NO, values, SequenceNumbers.UNASSIGNED_SEQ_NO),
                get(SearchHit.Fields._PRIMARY_TERM, values, SequenceNumbers.UNASSIGNED_PRIMARY_TERM),
                get(SourceFieldMapper.NAME, values, null),
                get(SearchHit.Fields.HIGHLIGHT, values, null),
                get(SearchHit.Fields.SORT, values, SearchSortValues.EMPTY),
                get(SearchHit.Fields.MATCHED_QUERIES, values, null),
                get(SearchHit.Fields._EXPLANATION, values, null),
                shardTarget,
                index,
                clusterAlias,
                get(SearchHit.Fields.INNER_HITS, values, null),
                get(SearchHit.DOCUMENT_FIELDS, values, Collections.emptyMap()),
                get(SearchHit.METADATA_FIELDS, values, Collections.emptyMap()),
                ALWAYS_REFERENCED // TODO: do we ever want pooling here?
        );
    }

    @SuppressWarnings("unchecked")
    private static <T> T get(String key, Map<String, Object> map, T defaultValue) {
        return (T) map.getOrDefault(key, defaultValue);
    }

    private static float parseScore(XContentParser parser) throws IOException {
        if (parser.currentToken() == XContentParser.Token.VALUE_NUMBER || parser.currentToken() == XContentParser.Token.VALUE_STRING) {
            return parser.floatValue();
        } else {
            return Float.NaN;
        }
    }

    private static BytesReference parseSourceBytes(XContentParser parser) throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(parser.contentType().xContent())) {
            // the original document gets slightly modified: whitespaces or
            // pretty printing are not preserved,
            // it all depends on the current builder settings
            builder.copyCurrentStructure(parser);
            return BytesReference.bytes(builder);
        }
    }

    private static Map<String, DocumentField> parseFields(XContentParser parser) throws IOException {
        Map<String, DocumentField> fields = new HashMap<>();
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            DocumentField field = DocumentField.fromXContent(parser);
            fields.put(field.getName(), field);
        }
        return fields;
    }

    private static Map<String, SearchHits> parseInnerHits(XContentParser parser) throws IOException {
        Map<String, SearchHits> innerHits = new HashMap<>();
        while ((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
            String name = parser.currentName();
            ensureExpectedToken(Token.START_OBJECT, parser.nextToken(), parser);
            ensureFieldName(parser, parser.nextToken(), SearchHits.Fields.HITS);
            innerHits.put(name, ParsedSearchHits.fromXContent(parser));
            ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
        }
        return innerHits;
    }

    private static Map<String, HighlightField> parseHighlightFields(XContentParser parser) throws IOException {
        Map<String, HighlightField> highlightFields = new HashMap<>();
        while ((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            HighlightField highlightField = HighlightField.fromXContent(parser);
            highlightFields.put(highlightField.name(), highlightField);
        }
        return highlightFields;
    }

    private static Explanation parseExplanation(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        XContentParser.Token token;
        Float value = null;
        String description = null;
        List<Explanation> details = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
            String currentFieldName = parser.currentName();
            token = parser.nextToken();
            if (SearchHit.Fields.VALUE.equals(currentFieldName)) {
                value = parser.floatValue();
            } else if (SearchHit.Fields.DESCRIPTION.equals(currentFieldName)) {
                description = parser.textOrNull();
            } else if (SearchHit.Fields.DETAILS.equals(currentFieldName)) {
                ensureExpectedToken(XContentParser.Token.START_ARRAY, token, parser);
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    details.add(parseExplanation(parser));
                }
            } else {
                parser.skipChildren();
            }
        }
        if (value == null) {
            throw new ParsingException(parser.getTokenLocation(), "missing explanation value");
        }
        if (description == null) {
            throw new ParsingException(parser.getTokenLocation(), "missing explanation description");
        }
        return Explanation.match(value, description, details);
    }

    /**
     * Encapsulates the nested identity of a hit.
     */
    public static final class ParsedNestedIdentity {

        private static final ConstructingObjectParser<SearchHit.NestedIdentity, Void> PARSER = new ConstructingObjectParser<>(
                "nested_identity",
                true,
                ctorArgs -> new SearchHit.NestedIdentity((String) ctorArgs[0], (int) ctorArgs[1], (SearchHit.NestedIdentity) ctorArgs[2])
        );
        static {
            PARSER.declareString(constructorArg(), new ParseField(SearchHit.NestedIdentity.FIELD));
            PARSER.declareInt(constructorArg(), new ParseField(SearchHit.NestedIdentity.OFFSET));
            PARSER.declareObject(optionalConstructorArg(), PARSER, new ParseField(SearchHit.NestedIdentity._NESTED));
        }

        static SearchHit.NestedIdentity fromXContent(XContentParser parser, Void context) {
            return fromXContent(parser);
        }

        public static SearchHit.NestedIdentity fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }
    }
}
