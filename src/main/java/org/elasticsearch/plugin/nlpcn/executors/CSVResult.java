package org.elasticsearch.plugin.nlpcn.executors;

import java.util.List;

/**
 * Created by Eliran on 27/12/2015.
 */
public class CSVResult {
    private final List<String> headers;
    private final List<String> lines;
    private long count; //对于聚合类型，返回数量为 limit 数量

    public CSVResult(List<String> headers, List<String> lines, long count) {
        this.headers = headers;
        this.lines = lines;
        this.count = count;
    }

    public CSVResult(List<String> headers, List<String> lines) {
        this.headers = headers;
        this.lines = lines;
    }

    public List<String> getHeaders() {
        return headers;
    }

    public List<String> getLines() {
        return lines;
    }

    public long getCount() {
        return count;
    }

}
