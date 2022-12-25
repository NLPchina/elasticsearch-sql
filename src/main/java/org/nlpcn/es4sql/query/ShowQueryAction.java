package org.nlpcn.es4sql.query;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequestBuilder;
import org.elasticsearch.client.internal.Client;
import org.nlpcn.es4sql.exception.SqlParseException;

/**
 * Created by Eliran on 6/10/2015.
 */
public class ShowQueryAction extends QueryAction {
    private String sql;
    public ShowQueryAction(Client client, String sql) {
        super(client,null);
        this.sql = sql;
    }

    @Override
    public SqlElasticRequestBuilder explain() throws SqlParseException {
        String sql = this.sql.replaceAll("\\s+"," ");
        //todo: support indices with space?
        String indexName = sql.split(" ")[1];
        final GetIndexRequestBuilder  indexRequestBuilder ;
        if (indexName.startsWith("<")) {
            if (!indexName.endsWith(">")) {
                int index = indexName.lastIndexOf('/');
                if (-1 < index) {
                    indexName = indexName.substring(0, index);
                }
            }
        } else if (indexName.contains("/")) {
            String[] indexAndType = indexName.split("\\/");
            indexName = indexAndType[0];
        }
        indexRequestBuilder = client.admin().indices().prepareGetIndex();
        indexRequestBuilder.addIndices(indexName);
        indexRequestBuilder.addFeatures(GetIndexRequest.Feature.MAPPINGS);

        return new SqlElasticRequestBuilder() {
            @Override
            public ActionRequest request() {
                return indexRequestBuilder.request();
            }

            @Override
            public String explain() {
                return indexRequestBuilder.toString();
            }

            @Override
            public ActionResponse get() {
                return indexRequestBuilder.get();
            }

            @Override
            public ActionRequestBuilder getBuilder() {
                return indexRequestBuilder;
            }
        };
    }
}
