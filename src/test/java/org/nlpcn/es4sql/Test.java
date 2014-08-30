package org.nlpcn.es4sql;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.nlpcn.commons.lang.index.MemoryIndex;
import org.nlpcn.commons.lang.index.MemoryIndex.Model;
import org.nlpcn.commons.lang.pinyin.Pinyin;
import org.nlpcn.commons.lang.util.IOUtil;

public class Test {
	public static void main(String[] args) throws IOException {
		BufferedReader reader = IOUtil.getReader("/Users/ansj/Documents/快盘/分词/library/baike.dic", "utf-8") ;
		String temp = null ;
		
		 MemoryIndex<String> mi = new MemoryIndex<String>(1, Model.PREX);
		
		 int i =0 ;
		while((temp=reader.readLine())!=null){
			 //生成各需要建立索引的字段
	        String jianpinpin = new String(Pinyin.str2FirstCharArr(temp)) ; //zg

	        //增加到索引中
	        mi.addItem(temp, temp ,jianpinpin);
	        System.out.println(i++);
	        
	        if(i>100000){
	        	break ;
	        }
	        
		}
		
		System.out.println("aaa");
		
	}
}
