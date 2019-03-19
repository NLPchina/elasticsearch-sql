package org.nlpcn.es4sql.domain;

public class KVValue implements Cloneable {
    public String key;
    public Object value;

    //zhongshu-comment 看样子，应该存在只有value没有key的情况
    public KVValue(Object value) {
        this.value = value;
    }

    public KVValue(String key, Object value) {
        if (key != null) {
            this.key = key.replace("'", "");
        }
        this.value = value;
    }

    @Override
    public String toString() {
        //zhongshu-comment 看样子，应该存在只有value没有key的情况
        if (key == null) {
            return value.toString();
        } else {
            return key + "=" + value;
        }
    }
}
