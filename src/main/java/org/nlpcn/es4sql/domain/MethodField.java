package org.nlpcn.es4sql.domain;

import java.util.ArrayList;
import java.util.List;

import org.nlpcn.commons.lang.util.StringUtil;
import org.nlpcn.es4sql.Util;

import com.alibaba.druid.sql.ast.SQLExpr;

/**
 * 搜索域
 * 
 * @author ansj
 *
 */
public class MethodField extends Field {
	private List<Object> params = null;

	public MethodField(String name, List<Object> params, String alias) {
		super(name, alias);
		this.params = params;
		if (StringUtil.isBlank(alias)) {
			this.setAlias(this.toString());
		}
	}

	public List<Object> getParams() {
		return params;
	}

	public void setParams(List<Object> params) {
		this.params = params;
	}

	@Override
	public String toString() {
		return this.name + "(" + Util.joiner(params, ",") + ")";
	}

	public static Field makeField(String name, List<SQLExpr> arguments, String alias) {
		List<Object> paramers = new ArrayList<Object>();

		for (SQLExpr object : arguments) {
			paramers.add(object.toString());
		}

		return new MethodField(name, paramers, alias);
	}
}
