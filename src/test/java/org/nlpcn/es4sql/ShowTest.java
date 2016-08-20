package org.nlpcn.es4sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.search.SearchHits;
import org.junit.Assert;
import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.parse.ElasticSqlExprParser;
import org.nlpcn.es4sql.query.SqlElasticRequestBuilder;
import org.nlpcn.es4sql.query.SqlElasticSearchRequestBuilder;

import java.io.IOException;
import java.sql.SQLFeatureNotSupportedException;

import static org.nlpcn.es4sql.TestsConstants.*;

/**
 * Created by Eliran on 16/10/2015.
 */
public class ShowTest {

    @Test
    public void showAll_atLeastOneIndexReturns() throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = "show *";
        GetIndexResponse getIndexResponse = runShowQuery(query);
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = getIndexResponse.getMappings();
        Assert.assertTrue(mappings.size() >= 1);

    }

    @Test
    public void showIndex_onlyOneIndexReturn() throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = "show "+ TEST_INDEX;
        GetIndexResponse getIndexResponse = runShowQuery(query);
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = getIndexResponse.getMappings();
        Assert.assertEquals(1, mappings.size());
        Assert.assertTrue(mappings.containsKey(TEST_INDEX));

    }
    @Test
    public void showIndex_onlyOneIndexReturWithMoreThanOneTypes() throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = "show "+ TEST_INDEX;
        GetIndexResponse getIndexResponse = runShowQuery(query);
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = getIndexResponse.getMappings();
        ImmutableOpenMap<String, MappingMetaData> typeToData = mappings.get(TEST_INDEX);
        Assert.assertTrue(typeToData.size()>1);
    }

    @Test
    public void showIndexType_onlyOneTypeReturn() throws SqlParseException, SQLFeatureNotSupportedException, IOException {
        String query = String.format("show %s/account", TEST_INDEX);
        GetIndexResponse getIndexResponse = runShowQuery(query);
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = getIndexResponse.getMappings();
        ImmutableOpenMap<String, MappingMetaData> typeToData = mappings.get(TEST_INDEX);
        Assert.assertEquals(1,typeToData.size());
    }

    private GetIndexResponse runShowQuery(String query) throws SqlParseException, SQLFeatureNotSupportedException, SQLFeatureNotSupportedException {
        SearchDao searchDao = MainTestSuite.getSearchDao();
        SqlElasticRequestBuilder requestBuilder =  searchDao.explain(query).explain();
        return (GetIndexResponse) requestBuilder.get();
    }
}
