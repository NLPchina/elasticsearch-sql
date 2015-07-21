package org.nlpcn.es4sql;

import java.sql.SQLFeatureNotSupportedException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.durid.sql.SQLUtils;
import org.durid.sql.ast.expr.SQLQueryExpr;
import org.durid.sql.ast.statement.SQLTableSource;
import org.durid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.ESActionFactory;
import org.nlpcn.es4sql.query.QueryAction;


public class SearchDao {

	private static final Set<String> END_TABLE_MAP = new HashSet<>();

	static {
		END_TABLE_MAP.add("limit");
		END_TABLE_MAP.add("order");
		END_TABLE_MAP.add("where");
		END_TABLE_MAP.add("group");

	}

	private Client client = null;


	public SearchDao(Client client) {
		this.client = client;
	}


	/**
	 * Prepare action And transform sql
	 * into ES ActionRequest
	 * @param sql SQL query to execute.
	 * @return ES request
	 * @throws SqlParseException
	 */
	public ActionRequestBuilder explain(String sql) throws SqlParseException, SQLFeatureNotSupportedException {
		final String pattern = ".*((group\\sby[\\s\\w,]*)(\\(.\\S*\\))([\\s\\w]*.*))";
		Pattern p = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
		Matcher m = p.matcher(sql);

		String subAggregate = "";
		if(m.find()) {
			subAggregate = m.group(3);
			// If we find a sub aggregate possibility
			if(m.group(2).trim().equalsIgnoreCase("group by")) {
				sql = sql.replace(m.group(1), "");
				// Is there more at the end of the query?
				if(m.group(4).trim().length() > 0) {
					// Add it back on
					sql += " " + m.group(4);
				}
			}
		}
		QueryAction query = ESActionFactory.create(client, sql);
		ActionRequestBuilder ret = query.explain();

		// If there were aggregations, lets add them now
		if(subAggregate.trim().length() > 0) {
			addAggregations(subAggregate, ret);
		}

		return ret;
	}

	private void addAggregations(String aggs, ActionRequestBuilder actionRequestBuilder) {
		// If the aggs parameter isn't null or empty
		if (aggs != null && aggs.trim().length() > 0) {
			// Get a reference to Action Request Builder
			SearchRequestBuilder b = (SearchRequestBuilder) actionRequestBuilder;

			// Split the comma delimited field names
			String[] arAggs = aggs.split(",");
			for (String strAgg : arAggs) {
				strAgg = strAgg.trim();

				if(strAgg.startsWith("(")) {
					strAgg = strAgg.substring(1, strAgg.length());
				}

				// Split again on left parenthesis for sub aggregates
				String[] arSubAggs = strAgg.split("\\(");
				TermsBuilder objOriginalTerms = null;
				for (String strSubAgg: arSubAggs) {
					strSubAgg = strSubAgg.replace(")", "");
					if (objOriginalTerms == null) {
						objOriginalTerms = AggregationBuilders.terms(strSubAgg).field(strSubAgg).size(0).order(Terms.Order.term(true));
						// Add our aggregation to the actionRequestBuilder
						b.addAggregation(objOriginalTerms);
					}
					else {
						TermsBuilder objSubTerms = AggregationBuilders.terms(strSubAgg).field(strSubAgg).size(0).order(Terms.Order.term(true));
						objOriginalTerms.subAggregation(objSubTerms);
						objOriginalTerms = objSubTerms;
					}
				}
			}
		}
	}

}
