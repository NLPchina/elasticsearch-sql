package org.nlpcn.es4sql.domain;

/**
 * 搜索域
 * 
 * @author ansj
 *
 */
public class Field {
	
	protected String name;
	private String alias;

	public Field(String name, String alias) {
		this.name = name;
		this.alias = alias;
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

}
