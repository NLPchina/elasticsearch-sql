package org.nlpcn.es4sql.query;


import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.client.Client;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.nlpcn.es4sql.domain.Delete;
import org.nlpcn.es4sql.domain.Where;
import org.nlpcn.es4sql.domain.hints.Hint;
import org.nlpcn.es4sql.domain.hints.HintType;
import org.nlpcn.es4sql.exception.SqlParseException;

import org.nlpcn.es4sql.query.maker.QueryMaker;

public class DeleteQueryAction extends QueryAction {

	private final Delete delete;
	private DeleteByQueryRequestBuilder request;

	public DeleteQueryAction(Client client, Delete delete) {
		super(client, delete);
		this.delete = delete;
	}

	@Override
	public SqlElasticDeleteByQueryRequestBuilder explain() throws SqlParseException {
		this.request = new DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE);

		setIndicesAndTypes();
		setWhere(delete.getWhere());

		// maximum number of processed documents
		if (delete.getRowCount() > -1) {
			request.size(delete.getRowCount());
		}

		// set conflicts param
		updateRequestWithConflicts();

        SqlElasticDeleteByQueryRequestBuilder deleteByQueryRequestBuilder = new SqlElasticDeleteByQueryRequestBuilder(request);
		return deleteByQueryRequestBuilder;
	}


	/**
	 * Set indices and types to the delete by query request.
	 */
	private void setIndicesAndTypes() {

        DeleteByQueryRequest innerRequest = request.request();
        innerRequest.indices(query.getIndexArr());
        String[] typeArr = query.getTypeArr();
        if (typeArr!=null){
            innerRequest.getSearchRequest().types(typeArr);
        }
//		String[] typeArr = query.getTypeArr();
//		if (typeArr != null) {
//            request.set(typeArr);
//		}
	}


	/**
	 * Create filters based on
	 * the Where clause.
	 *
	 * @param where the 'WHERE' part of the SQL query.
	 * @throws SqlParseException
	 */
	private void setWhere(Where where) throws SqlParseException {
		if (where != null) {
			QueryBuilder whereQuery = QueryMaker.explan(where);
			request.filter(whereQuery);
		} else {
			request.filter(QueryBuilders.matchAllQuery());
		}
	}

	private void updateRequestWithConflicts() {
		for (Hint hint : delete.getHints()) {
			if (hint.getType() == HintType.CONFLICTS && hint.getParams() != null && 0 < hint.getParams().length) {
				String conflicts = hint.getParams()[0].toString();
				switch (conflicts) {
					case "proceed": request.abortOnVersionConflict(false); return;
					case "abort": request.abortOnVersionConflict(true); return;
					default: throw new IllegalArgumentException("conflicts may only be \"proceed\" or \"abort\" but was [" + conflicts + "]");
				}
			}
		}
	}

}
