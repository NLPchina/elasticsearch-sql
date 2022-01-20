package org.nlpcn.es4sql.query;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.nlpcn.es4sql.domain.Query;
import org.nlpcn.es4sql.domain.Select;
import org.nlpcn.es4sql.domain.hints.Hint;
import org.nlpcn.es4sql.domain.hints.HintType;
import org.nlpcn.es4sql.exception.SqlParseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

/**
 * Abstract class. used to transform Select object (Represents SQL query) to
 * SearchRequestBuilder (Represents ES query)
 */
public abstract class QueryAction {

    protected org.nlpcn.es4sql.domain.Query query;
    protected Client client;

    public QueryAction(Client client, Query query) {
        this.client = client;
        this.query = query;
    }

    protected void updateRequestWithStats(Select select, SearchRequestBuilder request) {
        for (Hint hint : select.getHints()) {
            if (hint.getType() == HintType.STATS && hint.getParams() != null && 0 < hint.getParams().length) {
                request.setStats(Arrays.stream(hint.getParams()).map(Object::toString).toArray(String[]::new));
            }
        }
    }

    protected void updateRequestWithCollapse(Select select, SearchRequestBuilder request) throws SqlParseException {
        for (Hint hint : select.getHints()) {
            if (hint.getType() == HintType.COLLAPSE && hint.getParams() != null && 0 < hint.getParams().length) {
                try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, hint.getParams()[0].toString())) {
                    request.setCollapse(CollapseBuilder.fromXContent(parser));
                } catch (IOException e) {
                    throw new SqlParseException("could not parse collapse hint: " + e.getMessage());
                }
            }
        }
    }

    protected void updateRequestWithPostFilter(Select select, SearchRequestBuilder request) {
        for (Hint hint : select.getHints()) {
            if (hint.getType() == HintType.POST_FILTER && hint.getParams() != null && 0 < hint.getParams().length) {
                request.setPostFilter(QueryBuilders.wrapperQuery(hint.getParams()[0].toString()));
            }
        }
    }

    protected void updateRequestWithIndexAndRoutingOptions(Select select, SearchRequestBuilder request) {
        for (Hint hint : select.getHints()) {
            if (hint.getType() == HintType.IGNORE_UNAVAILABLE) {
                //saving the defaults from TransportClient search
                request.setIndicesOptions(IndicesOptions.fromOptions(true, false, true, false, IndicesOptions.strictExpandOpenAndForbidClosed()));
            }
            if (hint.getType() == HintType.ROUTINGS) {
                Object[] routings = hint.getParams();
                String[] routingsAsStringArray = new String[routings.length];
                for (int i = 0; i < routings.length; i++) {
                    routingsAsStringArray[i] = routings[i].toString();
                }
                request.setRouting(routingsAsStringArray);
            }
        }
    }

    protected void updateRequestWithPreference(Select select, SearchRequestBuilder request) {
        for (Hint hint : select.getHints()) {
            if (hint.getType() == HintType.PREFERENCE && hint.getParams() != null && 0 < hint.getParams().length) {
                request.setPreference(hint.getParams()[0].toString());
            }
        }
    }

    protected void updateRequestWithTrackTotalHits(Select select, SearchRequestBuilder request) {
        for (Hint hint : select.getHints()) {
            if (hint.getType() == HintType.TRACK_TOTAL_HITS && hint.getParams() != null && 0 < hint.getParams().length) {
                String param = hint.getParams()[0].toString();
                try {
                    request.setTrackTotalHitsUpTo(Integer.parseInt(param));
                } catch (NumberFormatException ex) {
                    request.setTrackTotalHits(Boolean.parseBoolean(param));
                }
            }
        }
    }

    protected void updateRequestWithTimeout(Select select, SearchRequestBuilder request) {
        for (Hint hint : select.getHints()) {
            if (hint.getType() == HintType.TIMEOUT && hint.getParams() != null && 0 < hint.getParams().length) {
                String param = hint.getParams()[0].toString();
                request.setTimeout(TimeValue.parseTimeValue(param, SearchSourceBuilder.TIMEOUT_FIELD.getPreferredName()));
            }
        }
    }

    protected void updateRequestWithIndicesOptions(Select select, SearchRequestBuilder request) throws SqlParseException {
        for (Hint hint : select.getHints()) {
            if (hint.getType() == HintType.INDICES_OPTIONS && hint.getParams() != null && 0 < hint.getParams().length) {
                String param = hint.getParams()[0].toString();
                try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, param)) {
                    request.setIndicesOptions(IndicesOptions.fromMap(parser.map(), SearchRequest.DEFAULT_INDICES_OPTIONS));
                } catch (IOException e) {
                    throw new SqlParseException("could not parse indices_options hint: " + e.getMessage());
                }
            }
        }
    }

    protected void updateRequestWithMinScore(Select select, SearchRequestBuilder request) {
        for (Hint hint : select.getHints()) {
            if (hint.getType() == HintType.MIN_SCORE && hint.getParams() != null && 0 < hint.getParams().length) {
                String param = hint.getParams()[0].toString();
                request.setMinScore(Float.parseFloat(param));
            }
        }
    }

    protected void updateRequestWithSearchAfter(Select select, SearchRequestBuilder request) {
        for (Hint hint : select.getHints()) {
            if (hint.getType() == HintType.SEARCH_AFTER) {
                request.searchAfter(hint.getParams());
            }
        }
    }

    protected void updateRequestWithRuntimeMappings(Select select, SearchRequestBuilder request) throws SqlParseException {
        for (Hint hint : select.getHints()) {
            if (hint.getType() == HintType.RUNTIME_MAPPINGS && hint.getParams() != null && 0 < hint.getParams().length) {
                try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, hint.getParams()[0].toString())) {
                    request.setRuntimeMappings(parser.map());
                } catch (IOException e) {
                    throw new SqlParseException("could not parse runtime_mappings hint: " + e.getMessage());
                }
            }
        }
    }

    protected void updateRequestWithHighlight(Select select, SearchRequestBuilder request) {
        boolean foundAnyHighlights = false;
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        for (Hint hint : select.getHints()) {
            if (hint.getType() == HintType.HIGHLIGHT) {
                HighlightBuilder.Field highlightField = parseHighlightField(hint.getParams());
                if (highlightField != null) {
                    foundAnyHighlights = true;
                    highlightBuilder.field(highlightField);
                }
            }
        }
        if (foundAnyHighlights) {
            request.highlighter(highlightBuilder);
        }
    }

    protected HighlightBuilder.Field parseHighlightField(Object[] params)
    {
        if (params == null || params.length == 0 || params.length > 2) {
            //todo: exception.
        }
        HighlightBuilder.Field field = new HighlightBuilder.Field(params[0].toString());
        if (params.length == 1) {
            return field;
        }
        Map<String, Object> highlightParams = (Map<String, Object>) params[1];

        for (Map.Entry<String, Object> param : highlightParams.entrySet()) {
            switch (param.getKey()) {
                case "type":
                    field.highlighterType((String) param.getValue());
                    break;
                case "boundary_chars":
                    field.boundaryChars(fromArrayListToCharArray((ArrayList) param.getValue()));
                    break;
                case "boundary_max_scan":
                    field.boundaryMaxScan((Integer) param.getValue());
                    break;
                case "force_source":
                    field.forceSource((Boolean) param.getValue());
                    break;
                case "fragmenter":
                    field.fragmenter((String) param.getValue());
                    break;
                case "fragment_offset":
                    field.fragmentOffset((Integer) param.getValue());
                    break;
                case "fragment_size":
                    field.fragmentSize((Integer) param.getValue());
                    break;
                case "highlight_filter":
                    field.highlightFilter((Boolean) param.getValue());
                    break;
                case "matched_fields":
                    field.matchedFields((String[]) ((ArrayList) param.getValue()).toArray(new String[((ArrayList) param.getValue()).size()]));
                    break;
                case "no_match_size":
                    field.noMatchSize((Integer) param.getValue());
                    break;
                case "num_of_fragments":
                    field.numOfFragments((Integer) param.getValue());
                    break;
                case "order":
                    field.order((String) param.getValue());
                    break;
                case "phrase_limit":
                    field.phraseLimit((Integer) param.getValue());
                    break;
                case "post_tags":
                    field.postTags((String[]) ((ArrayList) param.getValue()).toArray(new String[((ArrayList) param.getValue()).size()]));
                    break;
                case "pre_tags":
                    field.preTags((String[]) ((ArrayList) param.getValue()).toArray(new String[((ArrayList) param.getValue()).size()]));
                    break;
                case "require_field_match":
                    field.requireFieldMatch((Boolean) param.getValue());
                    break;

            }
        }
        return field;
    }

    private char[] fromArrayListToCharArray(ArrayList arrayList) {
        char[] chars = new char[arrayList.size()];
        int i = 0;
        for (Object item : arrayList) {
            chars[i] = item.toString().charAt(0);
            i++;
        }
        return chars;
    }


    /**
     * Prepare the request, and return ES request.
     * zhongshu-comment 将sql字符串解析后的java对象，转换为es的查询请求对象
     *
     * @return ActionRequestBuilder (ES request)
     * @throws SqlParseException
     */
    public abstract SqlElasticRequestBuilder explain() throws SqlParseException;
}
