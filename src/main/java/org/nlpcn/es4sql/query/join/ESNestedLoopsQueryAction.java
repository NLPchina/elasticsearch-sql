package org.nlpcn.es4sql.query.join;

import org.elasticsearch.client.Client;
import org.nlpcn.es4sql.domain.Condition;
import org.nlpcn.es4sql.domain.JoinSelect;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.QueryAction;
import org.nlpcn.es4sql.query.SqlElasticRequestBuilder;

import java.util.List;

/**
 * Created by Eliran on 15/9/2015.
 */
public class ESNestedLoopsQueryAction extends ESJoinQueryAction {

    public ESNestedLoopsQueryAction(Client client, JoinSelect joinSelect) {
        super(client, joinSelect);
    }

    @Override
    protected void fillSpecificRequestBuilder(JoinRequestBuilder requestBuilder) throws SqlParseException {
        NestedLoopsElasticRequestBuilder nestedBuilder = (NestedLoopsElasticRequestBuilder) requestBuilder;
        List<Condition> connectedConditions = joinSelect.getConnectedConditions();

        for(Condition c : connectedConditions){
            nestedBuilder.addConditionMapping(c);
        }
    }

    @Override
    protected JoinRequestBuilder createSpecificBuilder() {
        return new NestedLoopsElasticRequestBuilder();
    }

    private String removeAlias(String field) {
        String alias = joinSelect.getFirstTable().getAlias();
        if(!field.startsWith(alias+"."))
            alias = joinSelect.getSecondTable().getAlias();
        return field.replace(alias+".","");
    }

}
