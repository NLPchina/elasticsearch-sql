package org.nlpcn.es4sql.jdbc;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

/**
 * Factory that creates a {@link ElasticsearchSchema}.
 *
 * <p>Allows a custom schema to be included in a model.json file.
 */
@SuppressWarnings("UnusedDeclaration")
public class ElasticsearchSchemaFactory implements SchemaFactory {

    public ElasticsearchSchemaFactory() {
    }

    @Override public Schema create(SchemaPlus parentSchema, String name,
                                   Map<String, Object> operand) {
        final Map map = (Map) operand;

        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);

        try {
            final Map<String, Integer> coordinates =
                    mapper.readValue((String) map.get("coordinates"),
                            new TypeReference<Map<String, Integer>>() { });
            final Map<String, String> userConfig =
                    mapper.readValue((String) map.get("userConfig"),
                            new TypeReference<Map<String, String>>() { });
            final String index = (String) map.get("index");
            return new ElasticsearchSchema(coordinates, userConfig, index);
        } catch (IOException e) {
            throw new RuntimeException("Cannot parse values from json", e);
        }
    }
}
