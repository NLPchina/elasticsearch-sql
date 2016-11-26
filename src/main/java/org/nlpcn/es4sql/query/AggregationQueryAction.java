package org.nlpcn.es4sql.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.nested.NestedBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.nlpcn.es4sql.domain.Field;
import org.nlpcn.es4sql.domain.KVValue;
import org.nlpcn.es4sql.domain.MethodField;
import org.nlpcn.es4sql.domain.Order;
import org.nlpcn.es4sql.domain.Select;
import org.nlpcn.es4sql.domain.Where;
import org.nlpcn.es4sql.domain.hints.Hint;
import org.nlpcn.es4sql.domain.hints.HintType;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.maker.AggMaker;
import org.nlpcn.es4sql.query.maker.QueryMaker;

/**
 * Transform SQL query to Elasticsearch aggregations query
 */
public class AggregationQueryAction extends QueryAction {

    private final Select select;
    private AggMaker aggMaker = new AggMaker();
    private SearchRequestBuilder request;

    public AggregationQueryAction(Client client, Select select) {
        super(client, select);
        this.select = select;
    }

    @Override
    public SqlElasticSearchRequestBuilder explain() throws SqlParseException {
        this.request = client.prepareSearch();

        setIndicesAndTypes();

        setWhere(select.getWhere());
        AggregationBuilder<?> lastAgg = null;

        for (List<Field> groupBy : select.getGroupBys()) {
            if (!groupBy.isEmpty()) {
                Field field = groupBy.get(0);


                //make groupby can reference to field alias
                lastAgg = getGroupAgg(field, select);

                if (lastAgg != null && lastAgg instanceof TermsBuilder && !(field instanceof MethodField)) {
                    //if limit size is too small, increasing shard  size is required
                    if (select.getRowCount() < 200) {
                        ((TermsBuilder) lastAgg).shardSize(2000);
                        for (Hint hint : select.getHints()) {
                            if (hint.getType() == HintType.SHARD_SIZE) {
                                if (hint.getParams() != null && hint.getParams().length != 0 && hint.getParams()[0] != null) {
                                    ((TermsBuilder) lastAgg).shardSize((Integer) hint.getParams()[0]);
                                }
                            }
                        }
                    }
                    ((TermsBuilder) lastAgg).size(select.getRowCount());
                }

                if (field.isNested()) {
                    AggregationBuilder nestedBuilder = createNestedAggregation(field);

                    if (insertFilterIfExistsAfter(lastAgg, groupBy, nestedBuilder, 1)) {
                        groupBy.remove(1);
                    } else {
                        nestedBuilder.subAggregation(lastAgg);
                    }

                    request.addAggregation(wrapNestedIfNeeded(nestedBuilder, field.isReverseNested()));
                } else if (field.isChildren()) {
                    AggregationBuilder childrenBuilder = createChildrenAggregation(field);

                    if (insertFilterIfExistsAfter(lastAgg, groupBy, childrenBuilder, 1)) {
                        groupBy.remove(1);
                    } else {
                        childrenBuilder.subAggregation(lastAgg);
                    }

                    request.addAggregation(childrenBuilder);
                } else {
                    request.addAggregation(lastAgg);
                }

                for (int i = 1; i < groupBy.size(); i++) {
                    field = groupBy.get(i);
                    AggregationBuilder<?> subAgg = getGroupAgg(field, select);
                    if (subAgg instanceof TermsBuilder && !(field instanceof MethodField)) {
                        ((TermsBuilder) subAgg).size(0);
                    }

                    if (field.isNested()) {
                        AggregationBuilder nestedBuilder = createNestedAggregation(field);

                        if (insertFilterIfExistsAfter(subAgg, groupBy, nestedBuilder, i + 1)) {
                            groupBy.remove(i + 1);
                            i++;
                        } else {
                            nestedBuilder.subAggregation(subAgg);
                        }

                        lastAgg.subAggregation(wrapNestedIfNeeded(nestedBuilder, field.isReverseNested()));
                    } else if (field.isChildren()) {
                        AggregationBuilder childrenBuilder = createChildrenAggregation(field);

                        if (insertFilterIfExistsAfter(subAgg, groupBy, childrenBuilder, i + 1)) {
                            groupBy.remove(i + 1);
                            i++;
                        } else {
                            childrenBuilder.subAggregation(subAgg);
                        }

                        lastAgg.subAggregation(childrenBuilder);
                    } else {
                        lastAgg.subAggregation(subAgg);
                    }

                    lastAgg = subAgg;
                }
            }
        }

        Map<String, KVValue> groupMap = aggMaker.getGroupMap();
        // add field
        if (select.getFields().size() > 0) {
            setFields(select.getFields());
            explanFields(request, select.getFields(), lastAgg);
        }

        // add order
        if (lastAgg != null && select.getOrderBys().size() > 0) {
            for (Order order : select.getOrderBys()) {
                KVValue temp = groupMap.get(order.getName());
                if (temp != null) {
                    TermsBuilder termsBuilder = (TermsBuilder) temp.value;
                    switch (temp.key) {
                        case "COUNT":
                            termsBuilder.order(Terms.Order.count(isASC(order)));
                            break;
                        case "KEY":
                            termsBuilder.order(Terms.Order.term(isASC(order)));
                            // add the sort to the request also so the results get sorted as well
                            request.addSort(order.getName(), SortOrder.valueOf(order.getType()));
                            break;
                        case "FIELD":
                            termsBuilder.order(Terms.Order.aggregation(order.getName(), isASC(order)));
                            break;
                        default:
                            throw new SqlParseException(order.getName() + " can not to order");
                    }
                } else {
                    request.addSort(order.getName(), SortOrder.valueOf(order.getType()));
                }
            }
        }

        setLimitFromHint(this.select.getHints());

        request.setSearchType(SearchType.DEFAULT);
        updateRequestWithIndexAndRoutingOptions(select, request);
        updateRequestWithHighlight(select, request);
        SqlElasticSearchRequestBuilder sqlElasticRequestBuilder = new SqlElasticSearchRequestBuilder(request);
        return sqlElasticRequestBuilder;
    }
    
    private AggregationBuilder<?> getGroupAgg(Field field, Select select2) throws SqlParseException {
        boolean refrence = false;
        AggregationBuilder<?> lastAgg = null;
        for (Field temp : select.getFields()) {
            if (temp instanceof MethodField && temp.getName().equals("script")) {
                MethodField scriptField = (MethodField) temp;
                for (KVValue kv : scriptField.getParams()) {
                    if (kv.value.equals(field.getName())) {
                        lastAgg = aggMaker.makeGroupAgg(scriptField);
                        refrence = true;
                        break;
                    }
                }
            }
        }

        if (!refrence) lastAgg = aggMaker.makeGroupAgg(field);
        
        return lastAgg;
    }

    private AbstractAggregationBuilder wrapNestedIfNeeded(AggregationBuilder nestedBuilder, boolean reverseNested) {
        if (!reverseNested) return nestedBuilder;
        if (reverseNested && !(nestedBuilder instanceof NestedBuilder)) return nestedBuilder;
        //we need to jump back to root
        return AggregationBuilders.reverseNested(nestedBuilder.getName() + "_REVERSED").subAggregation(nestedBuilder);
    }

    private AggregationBuilder createNestedAggregation(Field field) {
        AggregationBuilder nestedBuilder;

        String nestedPath = field.getNestedPath();

        if (field.isReverseNested()) {
            if (nestedPath == null || !nestedPath.startsWith("~"))
                return AggregationBuilders.reverseNested(getNestedAggName(field)).path(nestedPath);
            nestedPath = nestedPath.substring(1);
        }

        nestedBuilder = AggregationBuilders.nested(getNestedAggName(field)).path(nestedPath);

        return nestedBuilder;
    }

    private AggregationBuilder createChildrenAggregation(Field field) {
        AggregationBuilder childrenBuilder;

        String childType = field.getChildType();

        childrenBuilder = AggregationBuilders.children(getChildrenAggName(field)).childType(childType);

        return childrenBuilder;
    }

    private String getNestedAggName(Field field) {
        String prefix;

        if (field instanceof MethodField) {
            String nestedPath = field.getNestedPath();
            if (nestedPath != null) {
                prefix = nestedPath;
            } else {
                prefix = field.getAlias();
            }
        } else {
            prefix = field.getName();
        }
        return prefix + "@NESTED";
    }

    private String getChildrenAggName(Field field) {
        String prefix;

        if (field instanceof MethodField) {
            String childType = field.getChildType();

            if (childType != null) {
                prefix = childType;
            } else {
                prefix = field.getAlias();
            }
        } else {
            prefix = field.getName();
        }

        return prefix + "@CHILDREN";
    }

    private boolean insertFilterIfExistsAfter(AggregationBuilder<?> agg, List<Field> groupBy, AggregationBuilder builder, int nextPosition) throws SqlParseException {
        if (groupBy.size() <= nextPosition) return false;
        Field filterFieldCandidate = groupBy.get(nextPosition);
        if (!(filterFieldCandidate instanceof MethodField)) return false;
        MethodField methodField = (MethodField) filterFieldCandidate;
        if (!methodField.getName().toLowerCase().equals("filter")) return false;
        builder.subAggregation(aggMaker.makeGroupAgg(filterFieldCandidate).subAggregation(agg));
        return true;
    }

    private AggregationBuilder<?> updateAggIfNested(AggregationBuilder<?> lastAgg, Field field) {
        if (field.isNested()) {
            lastAgg = AggregationBuilders.nested(field.getName() + "Nested")
                    .path(field.getNestedPath())
                    .subAggregation(lastAgg);
        }
        return lastAgg;
    }

    private boolean isASC(Order order) {
        return "ASC".equals(order.getType());
    }

    private void setFields(List<Field> fields) {
        if (select.getFields().size() > 0) {
            ArrayList<String> includeFields = new ArrayList<>();

            for (Field field : fields) {
                if (field != null) {
                    includeFields.add(field.getName());
                }
            }

            request.setFetchSource(includeFields.toArray(new String[includeFields.size()]), null);
        }
    }

    private void explanFields(SearchRequestBuilder request, List<Field> fields, AggregationBuilder<?> groupByAgg) throws SqlParseException {
        for (Field field : fields) {
            if (field instanceof MethodField) {

                if (field.getName().equals("script")) {
                    request.addField(field.getAlias());
                    DefaultQueryAction defaultQueryAction = new DefaultQueryAction(client, select);
                    defaultQueryAction.intialize(request);
                    List<Field> tempFields = Lists.newArrayList(field);
                    defaultQueryAction.setFields(tempFields);
                    continue;
                }

                AbstractAggregationBuilder makeAgg = aggMaker.makeFieldAgg((MethodField) field, groupByAgg);
                if (groupByAgg != null) {
                    groupByAgg.subAggregation(makeAgg);
                } else {
                    request.addAggregation(makeAgg);
                }
            } else if (field instanceof Field) {
                request.addField(field.getName());
            } else {
                throw new SqlParseException("it did not support this field method " + field);
            }
        }
    }

    /**
     * Create filters based on
     * the Where clause.
     *
     * @param where the 'WHERE' part of the SQL query.
     * @throws SqlParseException
     */
    private void setWhere(Where where) throws SqlParseException {
        if (where != null) {
            QueryBuilder whereQuery = QueryMaker.explan(where,this.select.isQuery);
            request.setQuery(whereQuery);
        }
    }


    /**
     * Set indices and types to the search request.
     */
    private void setIndicesAndTypes() {
        request.setIndices(query.getIndexArr());

        String[] typeArr = query.getTypeArr();
        if (typeArr != null) {
            request.setTypes(typeArr);
        }
    }

    private void setLimitFromHint(List<Hint> hints) {
        int from = 0;
        int size = 0;
        for (Hint hint : hints) {
            if (hint.getType() == HintType.DOCS_WITH_AGGREGATION) {
                Integer[] params = (Integer[]) hint.getParams();
                if (params.length > 1) {
                    // if 2 or more are given, use the first as the from and the second as the size
                    // so it is the same as LIMIT from,size
                    // except written as /*! DOCS_WITH_AGGREGATION(from,size) */
                    from = params[0];
                    size = params[1];
                } else if (params.length == 1) {
                    // if only 1 parameter is given, use it as the size with a from of 0
                    size = params[0];
                }
                break;
            }
        }
        request.setFrom(from);
        request.setSize(size);
    }
}
