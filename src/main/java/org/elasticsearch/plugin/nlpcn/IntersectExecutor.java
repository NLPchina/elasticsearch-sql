package org.elasticsearch.plugin.nlpcn;

import com.google.common.collect.Maps;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.nlpcn.es4sql.domain.Field;
import org.nlpcn.es4sql.query.multi.MultiQueryRequestBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Intersect Executor
 */
public class IntersectExecutor implements ElasticHitsExecutor {

    private MultiQueryRequestBuilder builder;
    private SearchHits intersectHits;
    private String[] fieldsOrderFirstTable;
    private String[] fieldsOrderSecondTable;
    private String separator;

    public IntersectExecutor(MultiQueryRequestBuilder builder) {
        this.builder = builder;
        fillFieldsOrder();
        separator = UUID.randomUUID().toString();
    }

    @Override
    public void run() {
        ActionFuture<SearchResponse> first = this.builder.getFirstSearchRequest().execute();
        ActionFuture<SearchResponse> second = this.builder.getSecondSearchRequest().execute();

        //
        SearchHit[] hits = first.actionGet().getHits().getHits();
        Set<ComperableHitResult> firstResult = new LinkedHashSet<>();
        fillComparableSetFromHits(this.fieldsOrderFirstTable, hits, firstResult);

        //
        hits = second.actionGet().getHits().getHits();
        Set<ComperableHitResult> secondResult = new HashSet<>();
        fillComparableSetFromHits(this.fieldsOrderSecondTable, hits, secondResult);

        // retain
        firstResult.retainAll(secondResult);

        fillIntersectHitsFromResults(firstResult);
    }

    @Override
    public SearchHits getHits() {
        return this.intersectHits;
    }

    private void fillIntersectHitsFromResults(Set<ComperableHitResult> comparableHitResults) {
        int currentId = 1;
        List<SearchHit> intersectHitsList = new ArrayList<>(comparableHitResults.size());
        Set<Map.Entry<String, String>> firstTableFieldToAlias = this.builder.getFirstTableFieldToAlias().entrySet();
        for (ComperableHitResult result : comparableHitResults) {
            SearchHit originalHit = result.getOriginalHit();
            SearchHit searchHit = new SearchHit(currentId, originalHit.getId(), new Text(originalHit.getType()), originalHit.getFields(), null);
            searchHit.sourceRef(originalHit.getSourceRef());
            searchHit.getSourceAsMap().clear();
            Map<String, Object> sourceAsMap = result.getFlattenMap();
            for (Map.Entry<String, String> entry : firstTableFieldToAlias) {
                if (sourceAsMap.containsKey(entry.getKey())) {
                    Object value = sourceAsMap.get(entry.getKey());
                    sourceAsMap.remove(entry.getKey());
                    sourceAsMap.put(entry.getValue(), value);
                }
            }

            searchHit.getSourceAsMap().putAll(sourceAsMap);
            currentId++;
            intersectHitsList.add(searchHit);
        }
        int totalSize = currentId - 1;
        SearchHit[] unionHitsArr = intersectHitsList.toArray(new SearchHit[totalSize]);
        this.intersectHits = new SearchHits(unionHitsArr, new TotalHits(totalSize, TotalHits.Relation.EQUAL_TO), 1.0f);
    }

    private void fillComparableSetFromHits(String[] fieldsOrder, SearchHit[] hits, Set<ComperableHitResult> setToFill) {
        if (Objects.isNull(hits)) {
            return;
        }

        for (SearchHit hit : hits) {
            ComperableHitResult comperableHitResult = new ComperableHitResult(hit, fieldsOrder, this.separator);
            if (!comperableHitResult.isAllNull()) {
                setToFill.add(comperableHitResult);
            }
        }
    }

    private void fillFieldsOrder() {
        Map<String, String> firstTableFieldToAlias = this.builder.getFirstTableFieldToAlias();
        List<Field> firstTableFields = this.builder.getOriginalSelect(true).getFields();

        List<String> fieldsOrAliases = new ArrayList<>();
        for (Field field : firstTableFields) {
            if (firstTableFieldToAlias.containsKey(field.getName())) {
                fieldsOrAliases.add(field.getAlias());
            } else {
                fieldsOrAliases.add(field.getName());
            }
        }
        Collections.sort(fieldsOrAliases);

        int fieldsSize = fieldsOrAliases.size();

        this.fieldsOrderFirstTable = new String[fieldsSize];
        fillFieldsArray(fieldsOrAliases, firstTableFieldToAlias, this.fieldsOrderFirstTable);

        this.fieldsOrderSecondTable = new String[fieldsSize];
        fillFieldsArray(fieldsOrAliases, this.builder.getSecondTableFieldToAlias(), this.fieldsOrderSecondTable);
    }

    private void fillFieldsArray(List<String> fieldsOrAliases, Map<String, String> fieldsToAlias, String[] fields) {
        Map<String, String> aliasToField = inverseMap(fieldsToAlias);
        for (int i = 0, len = fields.length; i < len; i++) {
            String field = fieldsOrAliases.get(i);
            if (aliasToField.containsKey(field)) {
                field = aliasToField.get(field);
            }
            fields[i] = field;
        }
    }

    private Map<String, String> inverseMap(Map<String, String> mapToInverse) {
        Map<String, String> inverseMap = Maps.newHashMapWithExpectedSize(mapToInverse.size());
        for (Map.Entry<String, String> entry : mapToInverse.entrySet()) {
            inverseMap.put(entry.getValue(), entry.getKey());
        }
        return inverseMap;
    }
}
