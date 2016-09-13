package org.nlpcn.es4sql.domain;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.nlpcn.es4sql.Util;

/**
 * 搜索域
 * 
 * @author ansj
 *
 */
public class MethodField extends Field {
	private List<KVValue> params = null;
	private String option;

	public MethodField(String name, List<KVValue> params, String option, String alias) {
		super(name, alias);
		this.params = params;
		this.option = option;
		if (alias==null||alias.trim().length()==0) {
			this.setAlias(this.toString());
		}
	}

	public List<KVValue> getParams() {
		return params;
	}

    public Map<String,Object> getParamsAsMap(){
        Map<String,Object> paramsAsMap = new HashMap<>();
        for(KVValue kvValue : this.params){
            paramsAsMap.put(kvValue.key,kvValue.value);
        }
        return paramsAsMap;
    }

	@Override
	public String toString() {
		if (option != null) {
			return this.name + "(" + option + " " + Util.joiner(params, ",") + ")";
		}
		return this.name + "(" + Util.joiner(params, ",") + ")";
	}

	public String getOption() {
		return option;
	}

	public void setOption(String option) {
		this.option = option;
	}

}
