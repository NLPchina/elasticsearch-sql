package org.nlpcn.es4sql.query;

import org.elasticsearch.client.Client;
import org.nlpcn.es4sql.domain.*;
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
        //TODO: on map entry change names to t1ToT2 and make sure of this order here (based on aliases)
        HashJoinElasticRequestBuilder hashRequest = new HashJoinElasticRequestBuilder();

        Select t1Select = joinSelect.getT1Select();
        List<Field> t1ConnectedFields = joinSelect.getT1ConnectedFields();
        addFieldsToSelectIfMissing(t1Select,t1ConnectedFields);
        DefaultQueryAction t1QueryAction = new DefaultQueryAction(client,t1Select);
        t1QueryAction.explain();
        hashRequest.setFirstTableRequest(t1QueryAction.getRequestBuilder());

        Select t2Select = joinSelect.getT2Select();
        List<Field> t2ConnectedFields = joinSelect.getT2ConnectedFields();
        addFieldsToSelectIfMissing(t2Select,t2ConnectedFields);
        DefaultQueryAction t2QueryAction = new DefaultQueryAction(client,t2Select);
        t2QueryAction.explain();
        hashRequest.setSecondTableRequest(t2QueryAction.getRequestBuilder());


        hashRequest.setFirstTableReturnedField(joinSelect.getT1SelectedFields());
        hashRequest.setSecondTableReturnedField(joinSelect.getT2SelectedFields());

        String t1Alias = joinSelect.getT1Alias();
        String t2Alias = joinSelect.getT2Alias();
        List<Map.Entry<Field,Field>> comparisonFields = new ArrayList<>();
        for(Condition condition : joinSelect.getConnectedConditions()){

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
        hashRequest.setT1ToT2FieldsComparison(comparisonFields);

        return hashRequest;
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
