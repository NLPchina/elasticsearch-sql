package org.nlpcn.es4sql.domain;


/**
 * Represents the from clause.
 * Contains index and type which the
 * query refer to.
 */
public class From {
	private String index;
	private String type;


	/**
 	 * Extract index and type from the 'from' string
	 * @param from The part after the FROM keyword.
	 */
	public From(String from) {
		String[] parts = from.split("/");
		this.index = parts[0].trim();
		if (parts.length == 2) {
			this.type = parts[1].trim();
		}
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
}
