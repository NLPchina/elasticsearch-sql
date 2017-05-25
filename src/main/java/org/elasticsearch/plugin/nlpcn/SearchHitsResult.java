package org.elasticsearch.plugin.nlpcn;

import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Eliran on 28/8/2015.
 */
public class SearchHitsResult {
    private List<SearchHit> searchHits;
    private boolean matchedWithOtherTable;

    public SearchHitsResult() {
        searchHits = new ArrayList<>();
    }

    public SearchHitsResult(List<SearchHit> searchHits, boolean matchedWithOtherTable) {
        this.searchHits = searchHits;
        this.matchedWithOtherTable = matchedWithOtherTable;
    }

    public List<SearchHit> getSearchHits() {
        return searchHits;
    }

    public void setSearchHits(List<SearchHit> searchHits) {
        this.searchHits = searchHits;
    }

    public boolean isMatchedWithOtherTable() {
        return matchedWithOtherTable;
    }

    public void setMatchedWithOtherTable(boolean matchedWithOtherTable) {
        this.matchedWithOtherTable = matchedWithOtherTable;
    }
}
