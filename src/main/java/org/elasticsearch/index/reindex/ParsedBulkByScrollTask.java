/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Task storing information about a currently running BulkByScroll request.
 *
 * When the request is not sliced, this task is the only task created, and starts an action to perform search requests.
 *
 * When the request is sliced, this task can either represent a coordinating task (using
 * {@link BulkByScrollTask#setWorkerCount(int)}) or a worker task that performs search queries (using
 * {@link BulkByScrollTask#setWorker(float, Integer)}).
 *
 * We don't always know if this task will be a leader or worker task when it's created, because if slices is set to "auto" it may
 * be either depending on the number of shards in the source indices. We figure that out when the request is handled and set it on this
 * class with {@link #setWorkerCount(int)} or {@link #setWorker(float, Integer)}.
 */
public class ParsedBulkByScrollTask {

    /**
     * Status of the reindex, update by query, or delete by query. While in
     * general we allow {@linkplain Task.Status} implementations to make
     * backwards incompatible changes to their {@link Task.Status#toXContent}
     * implementations, this one has become defacto standardized because Kibana
     * parses it. As such, we should be very careful about removing things from
     * this.
     */
    public static class ParsedStatus {

        static final ConstructingObjectParser<Tuple<Long, Long>, Void> RETRIES_PARSER = new ConstructingObjectParser<>(
                "bulk_by_scroll_task_status_retries",
                true,
                a -> new Tuple<>(((Long) a[0]), (Long) a[1])
        );
        static {
            RETRIES_PARSER.declareLong(constructorArg(), new ParseField(BulkByScrollTask.Status.RETRIES_BULK_FIELD));
            RETRIES_PARSER.declareLong(constructorArg(), new ParseField(BulkByScrollTask.Status.RETRIES_SEARCH_FIELD));
        }

        public static void declareFields(ObjectParser<? extends BulkByScrollTask.StatusBuilder, Void> parser) {
            parser.declareInt(BulkByScrollTask.StatusBuilder::setSliceId, new ParseField(BulkByScrollTask.Status.SLICE_ID_FIELD));
            parser.declareLong(BulkByScrollTask.StatusBuilder::setTotal, new ParseField(BulkByScrollTask.Status.TOTAL_FIELD));
            parser.declareLong(BulkByScrollTask.StatusBuilder::setUpdated, new ParseField(BulkByScrollTask.Status.UPDATED_FIELD));
            parser.declareLong(BulkByScrollTask.StatusBuilder::setCreated, new ParseField(BulkByScrollTask.Status.CREATED_FIELD));
            parser.declareLong(BulkByScrollTask.StatusBuilder::setDeleted, new ParseField(BulkByScrollTask.Status.DELETED_FIELD));
            parser.declareInt(BulkByScrollTask.StatusBuilder::setBatches, new ParseField(BulkByScrollTask.Status.BATCHES_FIELD));
            parser.declareLong(BulkByScrollTask.StatusBuilder::setVersionConflicts, new ParseField(BulkByScrollTask.Status.VERSION_CONFLICTS_FIELD));
            parser.declareLong(BulkByScrollTask.StatusBuilder::setNoops, new ParseField(BulkByScrollTask.Status.NOOPS_FIELD));
            parser.declareObject(BulkByScrollTask.StatusBuilder::setRetries, RETRIES_PARSER, new ParseField(BulkByScrollTask.Status.RETRIES_FIELD));
            parser.declareLong(BulkByScrollTask.StatusBuilder::setThrottled, new ParseField(BulkByScrollTask.Status.THROTTLED_RAW_FIELD));
            parser.declareFloat(BulkByScrollTask.StatusBuilder::setRequestsPerSecond, new ParseField(BulkByScrollTask.Status.REQUESTS_PER_SEC_FIELD));
            parser.declareString(BulkByScrollTask.StatusBuilder::setReasonCancelled, new ParseField(BulkByScrollTask.Status.CANCELED_FIELD));
            parser.declareLong(BulkByScrollTask.StatusBuilder::setThrottledUntil, new ParseField(BulkByScrollTask.Status.THROTTLED_UNTIL_RAW_FIELD));
            parser.declareObjectArray(
                    BulkByScrollTask.StatusBuilder::setSliceStatuses,
                    (p, c) -> ParsedStatusOrException.fromXContent(p),
                    new ParseField(BulkByScrollTask.Status.SLICES_FIELD)
            );
        }

        public static BulkByScrollTask.Status fromXContent(XContentParser parser) throws IOException {
            XContentParser.Token token;
            if (parser.currentToken() == Token.START_OBJECT) {
                token = parser.nextToken();
            } else {
                token = parser.nextToken();
            }
            ensureExpectedToken(Token.START_OBJECT, token, parser);
            token = parser.nextToken();
            ensureExpectedToken(Token.FIELD_NAME, token, parser);
            return innerFromXContent(parser);
        }

        public static BulkByScrollTask.Status innerFromXContent(XContentParser parser) throws IOException {
            Token token = parser.currentToken();
            String fieldName = parser.currentName();
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
            BulkByScrollTask.StatusBuilder builder = new BulkByScrollTask.StatusBuilder();
            while ((token = parser.nextToken()) != Token.END_OBJECT) {
                if (token == Token.FIELD_NAME) {
                    fieldName = parser.currentName();
                } else if (token == Token.START_OBJECT) {
                    if (fieldName.equals(BulkByScrollTask.Status.RETRIES_FIELD)) {
                        builder.setRetries(ParsedStatus.RETRIES_PARSER.parse(parser, null));
                    } else {
                        parser.skipChildren();
                    }
                } else if (token == Token.START_ARRAY) {
                    if (fieldName.equals(BulkByScrollTask.Status.SLICES_FIELD)) {
                        while ((token = parser.nextToken()) != Token.END_ARRAY) {
                            builder.addToSliceStatuses(ParsedStatusOrException.fromXContent(parser));
                        }
                    } else {
                        parser.skipChildren();
                    }
                } else { // else if it is a value
                    switch (fieldName) {
                        case BulkByScrollTask.Status.SLICE_ID_FIELD -> builder.setSliceId(parser.intValue());
                        case BulkByScrollTask.Status.TOTAL_FIELD -> builder.setTotal(parser.longValue());
                        case BulkByScrollTask.Status.UPDATED_FIELD -> builder.setUpdated(parser.longValue());
                        case BulkByScrollTask.Status.CREATED_FIELD -> builder.setCreated(parser.longValue());
                        case BulkByScrollTask.Status.DELETED_FIELD -> builder.setDeleted(parser.longValue());
                        case BulkByScrollTask.Status.BATCHES_FIELD -> builder.setBatches(parser.intValue());
                        case BulkByScrollTask.Status.VERSION_CONFLICTS_FIELD -> builder.setVersionConflicts(parser.longValue());
                        case BulkByScrollTask.Status.NOOPS_FIELD -> builder.setNoops(parser.longValue());
                        case BulkByScrollTask.Status.THROTTLED_RAW_FIELD -> builder.setThrottled(parser.longValue());
                        case BulkByScrollTask.Status.REQUESTS_PER_SEC_FIELD -> builder.setRequestsPerSecond(parser.floatValue());
                        case BulkByScrollTask.Status.CANCELED_FIELD -> builder.setReasonCancelled(parser.text());
                        case BulkByScrollTask.Status.THROTTLED_UNTIL_RAW_FIELD -> builder.setThrottledUntil(parser.longValue());
                    }
                }
            }
            return builder.buildStatus();
        }
    }

    /**
     * The status of a slice of the request. Successful requests store the {@link StatusOrException#status} while failing requests store a
     * {@link StatusOrException#exception}.
     */
    public static class ParsedStatusOrException {

        /**
         * Since {@link StatusOrException} can contain either an {@link Exception} or a {@link Status} we need to peek
         * at a field first before deciding what needs to be parsed since the same object could contains either.
         * The {@link #EXPECTED_EXCEPTION_FIELDS} contains the fields that are expected when the serialised object
         * was an instance of exception and the {@link Status#FIELDS_SET} is the set of fields expected when the
         * serialized object was an instance of Status.
         */
        public static BulkByScrollTask.StatusOrException fromXContent(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.currentToken();
            if (token == null) {
                token = parser.nextToken();
            }
            if (token == Token.VALUE_NULL) {
                return null;
            } else {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
                token = parser.nextToken();
                // This loop is present only to ignore unknown tokens. It breaks as soon as we find a field
                // that is allowed.
                while (token != Token.END_OBJECT) {
                    ensureExpectedToken(Token.FIELD_NAME, token, parser);
                    String fieldName = parser.currentName();
                    // weird way to ignore unknown tokens
                    if (BulkByScrollTask.Status.FIELDS_SET.contains(fieldName)) {
                        return new BulkByScrollTask.StatusOrException(ParsedStatus.innerFromXContent(parser));
                    } else if (BulkByScrollTask.StatusOrException.EXPECTED_EXCEPTION_FIELDS.contains(fieldName)) {
                        return new BulkByScrollTask.StatusOrException(ElasticsearchException.innerFromXContent(parser, false));
                    } else {
                        // Ignore unknown tokens
                        token = parser.nextToken();
                        if (token == Token.START_OBJECT || token == Token.START_ARRAY) {
                            parser.skipChildren();
                        }
                        token = parser.nextToken();
                    }
                }
                throw new XContentParseException("Unable to parse StatusFromException. Expected fields not found.");
            }
        }
    }
}
