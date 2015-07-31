package org.nlpcn.es4sql.domain;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 将sql语句转换为select 对象
 * 
 * @author ansj
 */
public class Select extends Query {

	// Using this functions, will cause query to execute as aggregation.
	private final List<String> aggsFunctions = Arrays.asList("SUM", "MAX", "MIN", "AVG", "TOPHITS", "COUNT", "STATS");

	private List<Field> fields = new ArrayList<>();
	private List<List<Field>> groupBys = new ArrayList<>();
	private List<Order> orderBys = new ArrayList<>();
	private int offset;
	private int rowCount = 200;

	public boolean isQuery = false;

	public boolean isAgg = false;

	public Select() {
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

	public void addGroupBy(Field field) {
		List<Field> wrapper = new ArrayList<>();
		wrapper.add(field);
		addGroupBy(wrapper);
	}

	public void addGroupBy(List<Field> fields) {
		isAgg = true;
		this.groupBys.add(fields);
	}

	public List<List<Field>> getGroupBys() {
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

