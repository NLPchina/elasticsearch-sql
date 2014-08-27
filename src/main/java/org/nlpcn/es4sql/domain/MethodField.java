package org.nlpcn.es4sql.domain;

import java.util.ArrayList;
import java.util.List;

import org.nlpcn.commons.lang.util.StringUtil;
import org.nlpcn.es4sql.Util;
import org.nlpcn.es4sql.exception.SqlParseException;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;

/**
 * 搜索域
 * 
 * @author ansj
 *
 */
public class MethodField extends Field {
	private List<KVValue> params = null;

	public MethodField(String name, List<KVValue> params, String alias) {
		super(name, alias);
		this.params = params;
		if (StringUtil.isBlank(alias)) {
			this.setAlias(this.toString());
		}
	}

	public List<KVValue> getParams() {
		return params;
	}

	public void setParams(List<KVValue> params) {
		this.params = params;
	}

	@Override
	public String toString() {
		return this.name + "(" + Util.joiner(params, ",") + ")";
	}

	public static Field makeField(String name, List<SQLExpr> arguments, String alias) throws SqlParseException {
		List<KVValue> paramers = new ArrayList<>();
		for (SQLExpr object : arguments) {
			if (object instanceof SQLBinaryOpExpr) {
				SQLExpr right = ((SQLBinaryOpExpr) object).getRight();
				Object value = Util.expr2Object(right);
				paramers.add(new KVValue(((SQLBinaryOpExpr) object).getLeft().toString(), value));
			} else {
				paramers.add(new KVValue(Util.expr2Object(object)));
			}

		}
		return new MethodField(name, paramers, alias);
	}

}
