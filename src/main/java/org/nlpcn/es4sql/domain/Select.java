package org.nlpcn.es4sql.domain;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.nlpcn.commons.lang.util.StringUtil;
import org.nlpcn.es4sql.exception.SqlParseException;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;

/**
 * 将sql语句转换为select 对象
 * 
 * @author ansj
 */
public class Select {

	private List<Index> indexs = new LinkedList<>();
	private List<Field> fields = new LinkedList<>();
	private Where where = null;
	private List<String> groupBys = new LinkedList<>();
	private List<Order> orderBys = new LinkedList<>();
	private int offset;
	private int rowCount = Integer.MAX_VALUE;

	public boolean isQuery = false;

	public boolean isAgg = false;

	public Select() {
	}

	public List<Index> getIndexs() {
		return indexs;
	}

	public List<Field> getFields() {
		return fields;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public void setRowCount(int rowCount) {
		this.rowCount = rowCount;
	}

	public void addIndexAndType(String from) {
		if (StringUtil.isBlank(from)) {
			return;
		}
		indexs.add(new Index(from));
	}

	public void addGroupBy(String field) {
		if (StringUtil.isNotBlank(field)) {
			isAgg = true;
			this.groupBys.add(field);
		}
	}

	public Where getWhere() {
		return this.where;
	}

	public void setWhere(Where where) {
		this.where = where;
	}

	public List<String> getGroupBys() {
		return groupBys;
	}

	public List<Order> getOrderBys() {
		return orderBys;
	}

	public int getOffset() {
		return offset;
	}

	public int getRowCount() {
		return rowCount;
	}

	public void addOrderBy(String name, String type) {
		if ("_score".equals(name)) {
			isQuery = true;
		}
		this.orderBys.add(new Order(name, type));
	}

	public String[] getIndexArr() {
		String[] indexArr = new String[this.indexs.size()];
		for (int i = 0; i < indexArr.length; i++) {
			indexArr[i] = this.indexs.get(i).getIndex();
		}
		return indexArr;
	}

	public String[] getTypeArr() {
		List<String> list = new ArrayList<>();
		Index index = null;
		for (int i = 0; i < indexs.size(); i++) {
			index = indexs.get(i);
			if (index.getType() != null && !"*".equals(index.getType())) {
				list.add(index.getType());
			}
		}
		if (list.size() == 0) {
			return null;
		}

		return list.toArray(new String[list.size()]);
	}

	public void addField(String name, String alias) {
		if ("*".equals(name) || StringUtil.isBlank(name)) {
			return;
		}
		fields.add(new Field(name, alias));
	}

	public void addField(String name, List<SQLExpr> arguments, String alias) throws SqlParseException {
		isAgg = true ;
		fields.add(MethodField.makeField(name, arguments, alias));
	}


}
