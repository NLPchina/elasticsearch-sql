package org.nlpcn.es4sql;

import java.io.IOException;

import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;

import com.alibaba.druid.sql.SQLUtils;


public class ParserTest {
	
	@Test
	public void parser() throws IOException, SqlParseException{
		String sql = "(select        *,sum "
				+ "(   id   ),a.id,b.ic,fuck    (   sdfss) from (JAVA.*,bb.kk) filter bb <> c and  bb> =c and  sdfsd in (1,2,3,4,5,5) and sdfsd = analysis(sdfsdfsd) and  cc not like '%sdf%' and   (id <4) and ((id>4) or (a in (1,3) and b =323 and b =323 and b =323 and b =323 and b =323))  OR  ds ='s33' group by id,aa,df,sdf,sdf order by cc,dd desc,kk asc limit 3,10);";

		String formatMySql = SQLUtils.formatMySql(sql) ;
		
		System.out.println(formatMySql);
		
	}
}
