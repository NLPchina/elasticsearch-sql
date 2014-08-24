package org.nlpcn.es4sql.domain;

/**
 * 搜索域
 * @author ansj
 *
 */
public class Field {
	private String name;
	private String alias;
	 // 0 indefine, 1 function 2.subsql
	private int type;
	
	public Field(String name , String alias , int type){
		this.name = name ;
		this.alias = alias ;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getAlias() {
		return alias;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

}
