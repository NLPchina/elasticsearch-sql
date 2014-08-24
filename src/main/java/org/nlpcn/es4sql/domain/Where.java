package org.nlpcn.es4sql.domain;

import java.util.LinkedList;

public class Where {

	public static enum CONN {
		AND, OR
	};

	public static Where newInstance() {
		return new Where(CONN.AND);
	}

	private LinkedList<Where> wheres = new LinkedList<>();

	protected CONN conn;

	public Where(String connStr) {
		this.conn = CONN.valueOf(connStr.toUpperCase());
	}

	public Where(CONN conn) {
		this.conn = conn;
	}

	public void addWhere(Where where) {
		wheres.add(where);
	}

	public CONN getConn() {
		return this.conn;
	}
	
	public LinkedList<Where> getWheres() {
		return wheres;
	}

	@Override
	public String toString(){
		if(wheres.size()>0){
			String whereStr = wheres.toString() ;
			return this.conn + " ( "+whereStr.substring(1,whereStr.length()-1)+" ) " ;
		}else{
			return "" ;
		}
		
	}
	

}
