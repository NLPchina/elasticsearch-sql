/*
 * 
 * @author xuxiaolong
 * @date 2015年4月29日 下午5:39:27
 */
package com.jiuqi.bi.sql.metadata;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.jiuqi.bi.sql.DataTypes;
import com.jiuqi.bi.sql.DatabaseType;
import com.jiuqi.bi.util.StringUtils;

/**
 *  
 * @author xuxiaolong
 * @data 2015年4月29日 下午5:39:27
 */
public class SQLMetadataOthers implements ISQLMetadata {
	protected Connection conn;
	
	public SQLMetadataOthers(Connection conn) {
		if(conn == null) {
			throw new NullPointerException("数据库连接为空");
		}
		this.conn = conn;
	}

	public String getUser() throws SQLException, SQLMetadataException {
		return conn.getMetaData().getUserName();
	}

	public int getDataBaseType() {
		return DatabaseType.UNKNOWN;
	}

	public List<LogicTable> getUserTables() throws SQLException,
			SQLMetadataException {
		int type = LogicTable.LOGICTABLETYPE_SYNONYM | LogicTable.LOGICTABLETYPE_TABLE | LogicTable.LOGICTABLETYPE_VIEW;
		return getUserTables(type);
	}

	public List<LogicTable> getOtherTables() throws SQLException,
			SQLMetadataException {
		int type = LogicTable.LOGICTABLETYPE_SYNONYM | LogicTable.LOGICTABLETYPE_TABLE | LogicTable.LOGICTABLETYPE_VIEW;
		return getOtherTables(type);
	}

	public List<LogicTable> getUserTables(int type) throws SQLException,
			SQLMetadataException {
		List<LogicTable> result = new ArrayList<LogicTable>();
		if((LogicTable.LOGICTABLETYPE_TABLE & type) != 0) {//符合实体表查询条件
			List<LogicTable> databaseTables = getTables(new String[]{"TABLE"}, null, null);
			result.addAll(databaseTables);
		}
		if((LogicTable.LOGICTABLETYPE_VIEW & type) != 0) {//符合视图查询条件
			List<LogicTable> viewTables = getTables(new String[]{"VIEW"}, null, null);
			result.addAll(viewTables);
		}
		if((LogicTable.LOGICTABLETYPE_SYNONYM & type) != 0) {//符合同义词查询条件
			List<LogicTable> synonymTables = getTables(new String[]{"SYNONYM"}, null, null);
			result.addAll(synonymTables);
		}
		ComparatorTable ct = new ComparatorTable();
		Collections.sort(result, ct);
		return result;
	}

	public List<LogicTable> getOtherTables(int type) throws SQLException,
			SQLMetadataException {
		List<LogicTable> result = new ArrayList<LogicTable>();
		if((LogicTable.LOGICTABLETYPE_TABLE & type) != 0) {//符合实体表查询条件
			List<LogicTable> databaseTables = getTables(new String[]{"TABLE"}, null, null, false);
			result.addAll(databaseTables);
		}
		if((LogicTable.LOGICTABLETYPE_VIEW & type) != 0) {//符合视图查询条件
			List<LogicTable> viewTables = getTables(new String[]{"VIEW"}, null, null, false);
			result.addAll(viewTables);
		}
		if((LogicTable.LOGICTABLETYPE_SYNONYM & type) != 0) {//符合同义词查询条件
			List<LogicTable> synonymTables = getTables(new String[]{"SYNONYM"}, null, null, false);
			result.addAll(synonymTables);
		}
		
		DatabaseMetaData dbmd = conn.getMetaData();
		String defaultSchema = getSchema(dbmd, null);
		if(defaultSchema != null) {
			List<LogicTable> nr = new ArrayList<LogicTable>();
			for(LogicTable lt : result) {
				if(!defaultSchema.equals(lt.getOwner())) { //剔除自身默认的schema
					nr.add(lt);
				}
			}
			result = nr;
		}
		
		ComparatorTable ct = new ComparatorTable();
		Collections.sort(result, ct);
		return result;
	}

	public LogicTable getTableByName(String tableName) throws SQLException,
			SQLMetadataException {
		return getTableByName(tableName, null);
	}

	public LogicTable getTableByName(String tableName, String owner)
			throws SQLException, SQLMetadataException {
		List<LogicTable> result = new ArrayList<LogicTable>();
		
		result = getTables(new String[]{"TABLE"}, tableName, owner);
		if(result != null && result.size() > 0) {
			return result.get(0);
		}
		result = getTables(new String[]{"VIEW"}, tableName, owner);
		if(result != null && result.size() > 0) {
			return result.get(0);
		}
		result = getTables(new String[]{"SYNONYM"}, tableName, owner);
		if(result != null && result.size() > 0) {
			return result.get(0);
		}
		return null;
	}

	public List<LogicField> getFieldsByTableName(String tableName)
			throws SQLException, SQLMetadataException {
		return getFieldsByTableName(tableName, null);
	}

	public List<LogicField> getFieldsByTableName(String tableName, String owner)
			throws SQLException, SQLMetadataException {
		return getFields(tableName, owner, null);
	}

	public List<LogicIndex> getIndexesByTableName(String tableName)
			throws SQLException, SQLMetadataException {
		return getIndexesByTableName(tableName, null);
	}

	public LogicIndex getIndexByName(String name) throws SQLException,
			SQLMetadataException {
		return null;
	}

	public List<LogicIndex> getIndexesByTableName(String tableName, String owner)
			throws SQLException, SQLMetadataException {
		DatabaseMetaData dbmd = conn.getMetaData();
		ResultSet rs = dbmd.getIndexInfo(null, getSchema(dbmd, owner), tableName, false, false);
		try {
			List<LogicIndex> indexes = new ArrayList<LogicIndex>();
			while(rs.next()) {
				LogicIndex index = new LogicIndex();
				String table_name = rs.getString("TABLE_NAME");
				String index_name = rs.getString("INDEX_NAME");
				String column_name = rs.getString("COLUMN_NAME");
				String sort = rs.getString("ASC_OR_DESC");
				if(table_name != null && index_name != null && column_name != null) {
					boolean indexExsit = false;
					for(int i = 0; i < indexes.size(); i++) {
						if(index_name.equals(indexes.get(i).getIndexName())) {
							index = indexes.get(i);
							indexExsit = true;
							break;
						}
					}
					index.setTableName(tableName);
					index.setIndexName(index_name);
					LogicIndexField indexField = new LogicIndexField();
					indexField.setFieldName(column_name);
					if(sort.equals("A")) {
						indexField.setSortType(LogicIndexField.ASC);
					} else if(sort.equals("D")) {
						indexField.setSortType(LogicIndexField.DESC);
					}
					index.getIndexFields().add(indexField);
					if(!indexExsit) {
						indexes.add(index);
					}
				}
			}
			return indexes;
		} finally {
			rs.close();
		}
	}

	public LogicPrimaryKey getPrimaryKeyByTableName(String tableName)
			throws SQLException, SQLMetadataException {
		return getPrimaryKeyByTableName(tableName, null);
	}

	public LogicPrimaryKey getPrimaryKeyByName(String name)
			throws SQLException, SQLMetadataException {
		return null;
	}

	public LogicPrimaryKey getPrimaryKeyByTableName(String tableName,
			String owner) throws SQLException, SQLMetadataException {
		DatabaseMetaData dbmd = conn.getMetaData();
		ResultSet rs = dbmd.getPrimaryKeys(null, getSchema(dbmd, owner), tableName);
		try {
			LogicPrimaryKey pk = new LogicPrimaryKey();
			while(rs.next()) {
				String table_name = rs.getString("TABLE_NAME");
				String pk_name = rs.getString("PK_NAME");
				String column_name = rs.getString("COLUMN_NAME");
				if(table_name != null && pk_name != null && column_name != null) {
					pk.setPkName(pk_name);;
					pk.setTableName(table_name);
					pk.getFieldNames().add(column_name);
				}
			}
			if(StringUtils.isEmpty(pk.getPkName())) {
				return null;
			} else {
				return pk;
			}
		} finally {
			rs.close();
		}
	}

	public List<LogicProcedure> getUserProcedures() throws SQLException,
			SQLMetadataException {
		return getProcedures(null, null);
	}

	public List<LogicProcedure> getOtherProcedures() throws SQLException,
			SQLMetadataException {
		return new ArrayList<LogicProcedure>();
	}

	public LogicProcedure getProcedureByName(String name) throws SQLException,
			SQLMetadataException {
		if(StringUtils.isNotEmpty(name)) {
			return getProcedureByName(name, null);
		} else {
			return null;
		}
	}

	public LogicProcedure getProcedureByName(String name, String owner)
			throws SQLException, SQLMetadataException {
		if(StringUtils.isNotEmpty(name)) {
			List<LogicProcedure> lps = getProcedures(name, owner);
			if(lps.size() > 0) {
				return lps.get(0);
			}
		}
		return null;
	}

	public List<LogicProcedureParameter> getParametersByProcedureName(
			String procedureName) throws SQLException, SQLMetadataException {
		if(StringUtils.isEmpty(procedureName)) {
			return null;
		} else {
			return getParametersByProcedureName(procedureName, null);
		}
	}

	public List<LogicProcedureParameter> getParametersByProcedureName(
			String procedureName, String owner) throws SQLException,
			SQLMetadataException {
		if(StringUtils.isNotEmpty(procedureName)) {
			DatabaseMetaData dbmd = conn.getMetaData();
			String dbname = dbmd.getDatabaseProductName().toLowerCase();
			ResultSet rs = null;
			String userName = owner;
			if (dbname.indexOf("db2") != -1 && owner == null) {
				userName = dbmd.getUserName().toUpperCase();
			}
			rs = dbmd.getProcedureColumns(null, userName, procedureName, null);
			try {
				List<LogicProcedureParameter> lpps = new ArrayList<LogicProcedureParameter>();
				while(rs.next()) {
					LogicProcedureParameter lpp = new LogicProcedureParameter();
					int column_type = rs.getInt("COLUMN_TYPE");
					if(DatabaseMetaData.procedureColumnIn == column_type) {
						lpp.setMode(ProcedureParameterMode.IN);
					} else if(DatabaseMetaData.procedureColumnInOut == column_type) {
						lpp.setMode(ProcedureParameterMode.INOUT);
					} else if(DatabaseMetaData.procedureColumnOut == column_type) {
						lpp.setMode(ProcedureParameterMode.OUT);
					} else {
						continue;
					}
					lpp.setDataType(DataTypes.fromJavaSQLType(rs.getInt("DATA_TYPE")));
					lpp.setName(procedureName);
					lpps.add(lpp);
				}
				return lpps;
			} finally {
				rs.close();
			}
		}
		return null;
	}
	
	private static final String[] TABLE_TYPES = new String[] {"TABLE", "VIEW", "SYNONYM"};
	
	public List<LogicTable> getTables(String[] types, String name, String schema) throws SQLException {
		return getTables(types, name, schema, true);
	}
	
	private List<LogicTable> getTables(String[] types, String name, 
			String schema, boolean useDefaultSchema) throws SQLException {
		DatabaseMetaData dbmd = conn.getMetaData();
		if(schema == null && useDefaultSchema) {
			schema = getSchema(dbmd, schema);
		}
		
		ResultSet rs = dbmd.getTables(null, schema, name, types);
		try {
			List<LogicTable> tables = new ArrayList<LogicTable>();
			while (rs.next()) {
				LogicTable table = new LogicTable();
				String schemeName = rs.getString(2);
				String tableName = rs.getString(3);
				if (isRecyclebinTable(tableName))
					continue;
				table.setOwner(schemeName);
				table.setName(tableName);
				table.setDescription(rs.getString(5));
				String tableType = rs.getString(4);
				if (TABLE_TYPES[0].equalsIgnoreCase(tableType)) {
					table.setType(LogicTable.LOGICTABLETYPE_TABLE);
				} else if (TABLE_TYPES[1].equalsIgnoreCase(tableType)) {
					table.setType(LogicTable.LOGICTABLETYPE_VIEW);
				} else {
					table.setType(LogicTable.LOGICTABLETYPE_SYNONYM);
				}
				tables.add(table);
			}
			return tables;
		} finally {
			rs.close();
		}
	}
	
	private List<LogicField> getFields(String tableName, String schema, String fieldName) throws SQLException {
		DatabaseMetaData dbmd = conn.getMetaData();
		ResultSet rs = dbmd.getColumns(null, getSchema(dbmd, schema), tableName, fieldName);
		try {
			List<LogicField> fields = new ArrayList<LogicField>();
			while(rs.next()) {
				LogicField field = new LogicField();
				String fildName = rs.getString("COLUMN_NAME");
				field.setFieldName(fildName);
				field.setFieldTitle(rs.getString("REMARKS"));
				int nullAble = rs.getInt("NULLABLE");
				if(nullAble == 0) {
					field.setNullable(false);
				} else {
					field.setNullable(true);
				}
				field.setPrecision(rs.getInt("COLUMN_SIZE"));
				field.setScale(rs.getInt("DECIMAL_DIGITS"));
				field.setSize(rs.getInt("COLUMN_SIZE"));
				field.setDefaultValue(rs.getString("COLUMN_DEF"));
				field.setDataType(DataTypes.fromJavaSQLType(rs.getInt("DATA_TYPE")));
				fields.add(field);
			}
			return fields;
		} finally {
			rs.close();
		}
	}
	
	private boolean isRecyclebinTable(String tableName) {
		return tableName.startsWith("BIN$");
	}
	
	private List<LogicProcedure> getProcedures(String name, String schema) throws SQLException {
		DatabaseMetaData dbmd = conn.getMetaData();
		ResultSet rs = dbmd.getProcedures(null, getSchema(dbmd, schema), name);
		try {
			List<LogicProcedure> procedures = new ArrayList<LogicProcedure>();
			while(rs.next()) {
				LogicProcedure procedure = new LogicProcedure();
				procedure.setDescription(rs.getString("REMARKS"));
				String pName = rs.getString("PROCEDURE_NAME");
				int index = pName.indexOf(";");
				if(index > 0) {
					procedure.setName(pName.substring(0, index));
				} else {
					procedure.setName(pName);
				}
				procedure.setOwner(rs.getString("PROCEDURE_SCHEM"));
				procedures.add(procedure);
			}
			return procedures;
		} finally {
			rs.close();
		}
	}
	
	/**
	 * 获取数据库连接的schema信息<br/>
	 * 
	 * @param dbmd 数据库元信息
	 * @param schema 当前传入的schema，如果不为空，直接返回该schema
	 * @return
	 * @throws SQLException
	 */
	private String getSchema(DatabaseMetaData dbmd, String schema) throws SQLException {
		if(StringUtils.isNotEmpty(schema))
			return schema;
		
		String dbname = dbmd.getDatabaseProductName().toLowerCase();
		
		if (dbname.indexOf("db2") != -1) {
			return dbmd.getUserName().toUpperCase();
		}
		
		if (dbname.indexOf("impala") != -1) {
			//jdbc:impala://Host:Port[/Schema];Property1=Value;Property2=Value;...
			String url = dbmd.getURL();
			int pos1 = "jdbc:impala://".length()+1;
			int pos2 = url.indexOf('/', pos1);
			if(pos2 > 0) { //URL中包含了schema
				int pos3 = url.indexOf(';');
				if(pos3 == -1) {
					return url.substring(pos2+1);
				} else {
					return url.substring(pos2+1, pos3);
				}
			}
		}
		
		return null;
	}
	
}
