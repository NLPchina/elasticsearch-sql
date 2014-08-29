package org.nlpcn.es4sql.domain;

public class KVValue implements Cloneable {
	public String key;
	public Object value;

	public KVValue(Object value) {
		this.value = value;
	}

	public KVValue(String key, Object value) {
		this.key = key.replace("'", "");
		this.value = value;
	}

	@Override
	public String toString() {
		if (key == null) {
			return value.toString();
		} else {
			return key + "=" + value;
		}
	}
}
