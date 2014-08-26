package org.nlpcn.es4sql.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.internal.InternalSearchHit;

import com.alibaba.fastjson.JSONObject;

public class SearchResult {
	/**
	 * 查询结果
	 */
	private List<Map<String,Object>> results;

	private long total;

	double maxScore = 0;

	public SearchResult(SearchResponse resp) {
		SearchHits hits = resp.getHits();
		this.total = hits.getTotalHits();
		results = new ArrayList<>(hits.getHits().length);
		Map<String,Object> entry = null ;
		for (SearchHit searchHit : hits.getHits()) {
			System.out.println(searchHit.getSource());
			results.add(searchHit.getSource());
		}
	}

	public List<Map<String,Object>> getResults() {
		return results;
	}


	public void setResults(List<Map<String,Object>> results) {
		this.results = results;
	}


	public long getTotal() {
		return total;
	}

	public void setTotal(long total) {
		this.total = total;
	}

	public double getMaxScore() {
		return maxScore;
	}

	public void setMaxScore(double maxScore) {
		this.maxScore = maxScore;
	}

}
