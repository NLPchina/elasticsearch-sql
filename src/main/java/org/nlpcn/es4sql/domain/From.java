package org.nlpcn.es4sql.domain;


/**
 * @author zxh
 * @date 2018/9/27 18:42
 */
public class From {
	private String index;
	private String type;
    private String alias;

	/**
 	 * Extract index and type from the 'from' string
	 * @param from The part after the FROM keyword.
	 */
	public From(String from) {
		String[] parts = from.split("\\.");
		this.index = parts[0].trim();
		if (parts.length == 2) {
			this.type = parts[1].trim();
		}
	}

    public From(String from,String alias){
        this(from);
        this.alias = alias;
    }

	public String getIndex() {
		return index ;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }
}
