package org.nlpcn.es4sql.domain;

import org.elasticsearch.script.Script;
import org.elasticsearch.search.sort.SortOrder;

/**
 * 排序规则
 * 
 * @author ansj
 *
 */
public class Order {
	private String name;
	private String type;
	private SortOrder sortOrder;
	private Script script;

	public Order(String name, SortOrder sortOrder) {
		this.name = name;
		this.sortOrder = sortOrder;
	}

	public Order(String name, Script script, String type, SortOrder sortOrder) {
		this.name = name;
		this.script = script;
		this.type = type;
		this.sortOrder = sortOrder;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Script getScript() {
		return script;
	}

	public void setScript(Script script) {
		this.script = script;
	}

	public SortOrder getSortOrder() {
		return sortOrder;
	}

	public void setSortOrder(SortOrder sortOrder) {
		this.sortOrder = sortOrder;
	}

}
