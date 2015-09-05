package org.nlpcn.es4sql.query;

import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.client.Client;
import org.nlpcn.es4sql.domain.*;
import org.nlpcn.es4sql.domain.hints.Hint;
import org.nlpcn.es4sql.domain.hints.HintType;
import org.nlpcn.es4sql.exception.SqlParseException;

import java.util.*;

/**
 * Created by Eliran on 22/8/2015.
 */
public class ESHashJoinQueryAction extends QueryAction  {

    private JoinSelect joinSelect;

    public ESHashJoinQueryAction(Client client,JoinSelect joinSelect) {
        super(client, null);
        this.joinSelect = joinSelect;
    }

    @Override
    public SqlElasticRequestBuilder explain() throws SqlParseException {
        HashJoinElasticRequestBuilder hashRequest = new HashJoinElasticRequestBuilder();

        String t1Alias = joinSelect.getFirstTable().getAlias();
        String t2Alias = joinSelect.getSecondTable().getAlias();

        fillRequestBuilder(hashRequest.getFirstTable(), joinSelect.getFirstTable());
        fillRequestBuilder(hashRequest.getSecondTable(), joinSelect.getSecondTable());

        List<Map.Entry<Field, Field>> comparisonFields = getComparisonFields(t1Alias, t2Alias,joinSelect.getConnectedConditions());

        hashRequest.setT1ToT2FieldsComparison(comparisonFields);

        hashRequest.setJoinType(joinSelect.getJoinType());

        hashRequest.setTotalLimit(joinSelect.getTotalLimit());

        updateHashRequestWithHints(hashRequest);

        return hashRequest;
    }

    private void updateHashRequestWithHints(HashJoinElasticRequestBuilder hashRequest) {
        for(Hint hint : joinSelect.getHints()){
            if(hint.getType() == HintType.HASH_WITH_TERMS_FILTER) {
                hashRequest.setUseTermFiltersOptimization(true);
            }
            if(hint.getType() == HintType.JOIN_LIMIT){
                Object[] params = hint.getParams();
                hashRequest.getFirstTable().setHintLimit((Integer) params[0]);
                hashRequest.getSecondTable().setHintLimit((Integer) params[1]);
            }
        }

    }

    private List<Map.Entry<Field, Field>> getComparisonFields(String t1Alias, String t2Alias, List<Condition> connectedConditions) throws SqlParseException {
        List<Map.Entry<Field,Field>> comparisonFields = new ArrayList<>();
        for(Condition condition : connectedConditions){

            if(condition.getOpear() != Condition.OPEAR.EQ){
                throw new SqlParseException(String.format("HashJoin should only be with EQ conditions, got:%s on condition:%s", condition.getOpear().name(), condition.toString()));
            }

            String firstField = condition.getName();
            String secondField = condition.getValue().toString();
            Field t1Field,t2Field;
            if(firstField.startsWith(t1Alias)){
                t1Field = new Field(removeAlias(firstField,t1Alias),null);
                t2Field = new Field(removeAlias(secondField,t2Alias),null);
            }
            else {
                t1Field = new Field(removeAlias(secondField,t1Alias),null);
                t2Field = new Field(removeAlias(firstField,t2Alias),null);
            }
            comparisonFields.add(new AbstractMap.SimpleEntry<Field, Field>(t1Field, t2Field));
        }
        return comparisonFields;
    }

    private void fillRequestBuilder(TableInJoinRequestBuilder requestBuilder,TableOnJoinSelect tableOnJoinSelect) throws SqlParseException {
        List<Field> connectedFields = tableOnJoinSelect.getConnectedFields();
        addFieldsToSelectIfMissing(tableOnJoinSelect,connectedFields);
        requestBuilder.setOriginalSelect(tableOnJoinSelect);
        DefaultQueryAction queryAction = new DefaultQueryAction(client,tableOnJoinSelect);
        queryAction.explain();
        requestBuilder.setRequestBuilder(queryAction.getRequestBuilder());
        requestBuilder.setReturnedFields(tableOnJoinSelect.getSelectedFields());
        requestBuilder.setAlias(tableOnJoinSelect.getAlias());
    }

    private String removeAlias(String field, String alias) {
        return field.replace(alias+".","");
    }

    private void addFieldsToSelectIfMissing(Select select, List<Field> fields) {
        //this means all fields
        if(select.getFields() == null || select.getFields().size() == 0) return;

        List<Field> selectedFields = select.getFields();
        for(Field field : fields){
            if(!selectedFields.contains(field)){
                selectedFields.add(field);
            }
        }

    }
}
