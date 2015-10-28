package org.nlpcn.es4sql.domain;

import org.nlpcn.es4sql.domain.hints.Hint;
import org.nlpcn.es4sql.parse.SubQueryExpression;

import java.sql.Array;
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
	private final List<String> aggsFunctions = Arrays.asList("SUM", "MAX", "MIN", "AVG", "TOPHITS", "COUNT", "STATS","EXTENDED_STATS","PERCENTILES","SCRIPTED_METRIC");
    private List<Hint> hints = new ArrayList<>();
	private List<Field> fields = new ArrayList<>();
	private List<List<Field>> groupBys = new ArrayList<>();
	private List<Order> orderBys = new ArrayList<>();
	private int offset;
	private int rowCount = 200;
    private boolean containsSubQueries;
    private List<SubQueryExpression> subQueries;
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
		if (field == null || field.getName() == "*") {
			return;
		}

		if(field instanceof  MethodField && aggsFunctions.contains(field.getName().toUpperCase())) {
			isAgg = true;
		}

		fields.add(field);
	}

    public List<Hint> getHints() {
        return hints;
    }


    public void fillSubQueries() {
        subQueries = new ArrayList<>();
        Where where = this.getWhere();
        fillSubQueriesFromWhereRecursive(where);
    }

    private void fillSubQueriesFromWhereRecursive(Where where) {
        if(where == null) return;
        if(where instanceof Condition){
            Condition condition = (Condition) where;
            if ( condition.getValue() instanceof SubQueryExpression){
                this.subQueries.add((SubQueryExpression) condition.getValue());
                this.containsSubQueries = true;
            }
            if(condition.getValue() instanceof Object[]){

                for(Object o : (Object[]) condition.getValue()){
                    if ( o instanceof SubQueryExpression){
                        this.subQueries.add((SubQueryExpression) o);
                        this.containsSubQueries = true;
                    }
                }
            }
        }
        else {
            for(Where innerWhere : where.getWheres())
                fillSubQueriesFromWhereRecursive(innerWhere);
        }
    }

    public boolean containsSubQueries() {
        return containsSubQueries;
    }

    public List<SubQueryExpression> getSubQueries() {
        return subQueries;
    }
}

