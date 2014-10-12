package org.nlpcn.es4sql.domain;

import java.util.List;

import org.apache.lucene.queryparser.flexible.core.util.StringUtils;
import org.nlpcn.es4sql.Util;

/**
 * 搜索域
 * 
 * @author ansj
 *
 */
public class MethodField extends Field {
	private List<KVValue> params = null;
	private String option;

	public MethodField(String name, List<KVValue> params, String option, String alias) {
		super(name, alias);
		this.params = params;
		this.option = option;
		if (alias==null||alias.trim().length()==0) {
			this.setAlias(this.toString());
		}
	}

	public List<KVValue> getParams() {
		return params;
	}

	@Override
	public String toString() {
		if (option != null) {
			return this.name + "(" + option + " " + Util.joiner(params, ",") + ")";
		}
		return this.name + "(" + Util.joiner(params, ",") + ")";
	}

	public String getOption() {
		return option;
	}

	public void setOption(String option) {
		this.option = option;
	}

}
