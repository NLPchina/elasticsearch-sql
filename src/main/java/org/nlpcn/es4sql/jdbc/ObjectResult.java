package org.nlpcn.es4sql.jdbc;

import java.util.List;

/**
 * @author zxh
 * @date 2018/8/06 10:11
 */
public class ObjectResult {
    private final List<String> headers;
    private final List<List<Object>> lines;
    private long totalHits;

    public ObjectResult(List<String> headers, List<List<Object>> lines, long totalHits) {
        this.headers = headers;
        this.lines = lines;
        this.totalHits = totalHits;
    }

    public List<String> getHeaders() {
        return headers;
    }

    public List<List<Object>> getLines() {
        return lines;
    }

    public long getTotalHits(){
        return this.totalHits;
    }
}
