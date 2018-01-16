package org.nlpcn.es4sql.domain;

/**
 * 排序规则
 * @author ansj
 *
 */
public class Order {
	private String nestedPath;
	private String name;
	private String type;

	public Order(String nestedPath, String name, String type) {
        this.nestedPath = nestedPath;
		this.name = name;
		this.type = type;
	}

    public String getNestedPath() {
        return nestedPath;
    }

    public void setNestedPath(String nestedPath) {
        this.nestedPath = nestedPath;
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

}
