package org.nlpcn.es4sql.query.join;

import org.elasticsearch.client.Client;
import org.nlpcn.es4sql.domain.*;
import org.nlpcn.es4sql.domain.hints.Hint;
import org.nlpcn.es4sql.domain.hints.HintType;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.*;

import java.util.*;

/**
 * Created by Eliran on 22/8/2015.
 */
public class ESHashJoinQueryAction extends ESJoinQueryAction {

    public ESHashJoinQueryAction(Client client,JoinSelect joinSelect) {
        super(client, joinSelect);
    }

    @Override
    protected void fillSpecificRequestBuilder(JoinRequestBuilder requestBuilder) throws SqlParseException {
        String t1Alias = joinSelect.getFirstTable().getAlias();
        String t2Alias = joinSelect.getSecondTable().getAlias();

        List<Map.Entry<Field, Field>> comparisonFields = getComparisonFields(t1Alias, t2Alias,joinSelect.getConnectedConditions());

        ((HashJoinElasticRequestBuilder) requestBuilder).setT1ToT2FieldsComparison(comparisonFields);
    }

    @Override
    protected JoinRequestBuilder createSpecificBuilder() {
        return new HashJoinElasticRequestBuilder();
    }

    @Override
    protected void updateRequestWithHints(JoinRequestBuilder requestBuilder) {
        super.updateRequestWithHints(requestBuilder);
        for(Hint hint : joinSelect.getHints()){
            if(hint.getType() == HintType.HASH_WITH_TERMS_FILTER) {
                ((HashJoinElasticRequestBuilder) requestBuilder).setUseTermFiltersOptimization(true);
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

    private String removeAlias(String field, String alias) {
        return field.replace(alias+".","");
    }

}
