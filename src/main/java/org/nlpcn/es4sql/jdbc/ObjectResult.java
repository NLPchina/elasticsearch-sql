package org.nlpcn.es4sql.jdbc;

import java.util.List;

/**
 * Created by allwefantasy on 8/30/16.
 */
public class ObjectResult {
    private final List<String> headers;
    private final List<List<Object>> lines;

    public ObjectResult(List<String> headers, List<List<Object>> lines) {
        this.headers = headers;
        this.lines = lines;
    }

    public List<String> getHeaders() {
        return headers;
    }

    public List<List<Object>> getLines() {
        return lines;
    }
}
