package org.nlpcn.es4sql.query;

import java.lang.reflect.Method;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.nlpcn.es4sql.domain.MethodField;
import org.nlpcn.es4sql.domain.Select;
import org.nlpcn.es4sql.exception.SqlParseException;

/**
 * 查询封装类
 * 
 * @author ansj
 *
 */
public abstract class Query {

	protected Select select;

	protected Client client;

	protected SearchRequestBuilder request;

	public Query(Client client, Select select) {
		this.client = client;
		this.select = select;
	}

	/**
	 * 将select 解析成es的query
	 * 
	 * @throws SqlParseException
	 */
	public SearchRequestBuilder explan() throws SqlParseException {

		// set index
		request = client.prepareSearch(select.getIndexArr());

		// set type
		String[] typeArr = select.getTypeArr();
		if (typeArr != null) {
			request.setTypes(typeArr);
		}

		return _explan();
	}

	/**
	 * 针对不同类型的query进行不同类型的解析 fuck
	 * 
	 * @throws SqlParseException
	 */
	protected abstract SearchRequestBuilder _explan() throws SqlParseException;

}
