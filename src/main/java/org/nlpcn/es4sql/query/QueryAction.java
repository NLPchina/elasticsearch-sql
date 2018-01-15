package org.nlpcn.es4sql.query;

import com.fasterxml.jackson.core.JsonFactory;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.json.JsonXContentParser;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.nlpcn.es4sql.domain.Query;
import org.nlpcn.es4sql.domain.Select;
import org.nlpcn.es4sql.domain.hints.Hint;
import org.nlpcn.es4sql.domain.hints.HintType;
import org.nlpcn.es4sql.exception.SqlParseException;

import java.io.IOException;
import java.util.ArrayList;
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

    protected void updateRequestWithCollapse(Select select, SearchRequestBuilder request) throws SqlParseException {
        JsonFactory jsonFactory = new JsonFactory();
        for (Hint hint : select.getHints()) {
            if (hint.getType() == HintType.COLLAPSE && hint.getParams() != null && 0 < hint.getParams().length) {
                try (JsonXContentParser parser = new JsonXContentParser(NamedXContentRegistry.EMPTY, jsonFactory.createParser(hint.getParams()[0].toString()))) {
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
        for(Hint hint : select.getHints()){
            if(hint.getType() == HintType.IGNORE_UNAVAILABLE){
                //saving the defaults from TransportClient search
                request.setIndicesOptions(IndicesOptions.fromOptions(true, false, true, false, IndicesOptions.strictExpandOpenAndForbidClosed()));
            }
            if(hint.getType() == HintType.ROUTINGS){
                Object[] routings = hint.getParams();
                String[] routingsAsStringArray = new String[routings.length];
                for(int i=0;i<routings.length;i++){
                    routingsAsStringArray[i]=routings[i].toString();
                }
                request.setRouting(routingsAsStringArray);
            }
        }
    }

    protected void updateRequestWithHighlight(Select select, SearchRequestBuilder request) {
        boolean foundAnyHighlights = false;
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        for(Hint hint : select.getHints()){
            if(hint.getType() == HintType.HIGHLIGHT){
                HighlightBuilder.Field highlightField = parseHighlightField(hint.getParams());
                if(highlightField != null){
                    foundAnyHighlights = true;
                    highlightBuilder.field(highlightField);
                }
            }
        }
        if(foundAnyHighlights){
            request.highlighter(highlightBuilder);
        }
    }

    protected HighlightBuilder.Field parseHighlightField(Object[] params)
    {
        if(params == null || params.length == 0 || params.length > 2){
            //todo: exception.
        }
        HighlightBuilder.Field field = new HighlightBuilder.Field(params[0].toString());
        if(params.length == 1){
            return field;
        }
        Map<String,Object> highlightParams = (Map<String,Object>) params[1];

        for (Map.Entry<String,Object> param : highlightParams.entrySet()){
            switch (param.getKey()){
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
                    field.matchedFields((String[]) ((ArrayList)param.getValue()).toArray(new String[((ArrayList) param.getValue()).size()]));
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
                    field.postTags((String[]) ((ArrayList)param.getValue()).toArray(new String[((ArrayList) param.getValue()).size()]));
                    break;
                case "pre_tags":
                    field.preTags((String[]) ((ArrayList)param.getValue()).toArray(new String[((ArrayList) param.getValue()).size()]));
                    break;
                case "require_field_match":
                    field.requireFieldMatch((Boolean) param.getValue());
                    break;

            }
        }
        return field;
    }

    private char[] fromArrayListToCharArray(ArrayList arrayList){
        char[] chars = new char[arrayList.size()];
        int i=0;
        for(Object item : arrayList){
            chars[i] = item.toString().charAt(0);
            i++;
        }
        return chars;
    }


    /**
	 * Prepare the request, and return ES request.
	 * @return ActionRequestBuilder (ES request)
	 * @throws SqlParseException
	 */
	public abstract SqlElasticRequestBuilder explain() throws SqlParseException;
}
