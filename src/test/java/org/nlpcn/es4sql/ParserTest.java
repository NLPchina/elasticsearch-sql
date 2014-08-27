package org.nlpcn.es4sql;

import java.io.IOException;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.junit.Test;
import org.nlpcn.es4sql.domain.Condition;
import org.nlpcn.es4sql.domain.Paramer;
import org.nlpcn.es4sql.exception.SqlParseException;

import com.alibaba.druid.sql.SQLUtils;


public class ParserTest {
	
	@Test
	public void parser() throws IOException, SqlParseException{
		SearchDao searchDao = new SearchDao("127.0.0.1", 9300) ;
		
//	case "query":
//	case "matchQuery":
//	case "scoreQuery":
//	case "wildcardQuery":
//	case "matchPhraseQuery":
		
		SearchResponse select = searchDao.select("select age,account_number from bank where account_number>10 and age > 20 limit 10") ;
		
		
		
		System.out.println(select);
	}
}
