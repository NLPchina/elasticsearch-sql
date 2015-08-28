package org.elasticsearch.plugin.nlpcn;

import org.elasticsearch.search.internal.InternalSearchHit;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Eliran on 28/8/2015.
 */
public class SearchHitsResult {
    private List<InternalSearchHit> searchHits;
    private boolean matchedWithOtherTable;

    public SearchHitsResult() {
        searchHits = new ArrayList<>();
    }

    public SearchHitsResult(List<InternalSearchHit> searchHits, boolean matchedWithOtherTable) {
        this.searchHits = searchHits;
        this.matchedWithOtherTable = matchedWithOtherTable;
    }

    public List<InternalSearchHit> getSearchHits() {
        return searchHits;
    }

    public void setSearchHits(List<InternalSearchHit> searchHits) {
        this.searchHits = searchHits;
    }

    public boolean isMatchedWithOtherTable() {
        return matchedWithOtherTable;
    }

    public void setMatchedWithOtherTable(boolean matchedWithOtherTable) {
        this.matchedWithOtherTable = matchedWithOtherTable;
    }
}
