package org.nlpcn.es4sql;

import java.util.List;
import java.util.Map;

public class Util {
	public static String joiner(List<Object> lists, String oper) {
		if (lists.size() == 0) {
			return null;
		}

		StringBuilder sb = new StringBuilder(lists.get(0).toString());

		for (int i = 1; i < lists.size(); i++) {
			sb.append(oper);
			sb.append(lists.get(i).toString());
		}
		return sb.toString();
	}

	public static List<Map<String, Object>> sortByMap(List<Map<String, Object>> lists) {

		return lists;
	}
}
