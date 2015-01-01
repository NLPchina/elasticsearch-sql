package org.nlpcn.es4sql.domain;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 将sql语句转换为select 对象
 * 
 * @author ansj
 */
public class Select {

	// Using this functions, will cause query to execute as aggregation.
	private final List<String> aggsFunctions = Arrays.asList("SUM", "MAX", "MIN", "AVG", "TOPHITS", "COUNT", "STATS");

	private List<Index> indexs = new ArrayList<>();
	private List<Field> fields = new ArrayList<>();
	private Where where = null;
	private List<Field> groupBys = new ArrayList<>();
	private List<Order> orderBys = new ArrayList<>();
	private int offset;
	private int rowCount = 200;

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
		if (from == null || from.trim().length() == 0) {
			return;
		}
		indexs.add(new Index(from));
	}

	public void addGroupBy(Field field) {
		isAgg = true;
		this.groupBys.add(field);
	}

	public Where getWhere() {
		return this.where;
	}

	public void setWhere(Where where) {
		this.where = where;
	}

	public List<Field> getGroupBys() {
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
			if (index.getType() != null && index.getType().trim().length() > 0) {
				list.add(index.getType());
			}
		}
		if (list.size() == 0) {
			return null;
		}

		return list.toArray(new String[list.size()]);
	}

	public void addField(Field field) {
		if (field == null) {
			return;
		}

		if(field instanceof  MethodField && aggsFunctions.contains(field.getName())) {
			isAgg = true;
		}

		fields.add(field);
	}

}

