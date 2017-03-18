package com.cloudbeaver.client.dbbean;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.auth0.jwt.internal.com.fasterxml.jackson.annotation.JsonIgnore;
import com.cloudbeaver.client.common.BeaverFatalException;
import com.cloudbeaver.client.common.SqlHelper;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

/*
 * for now, only replace operator
 */
public class TransformOp {
	private String toColumn;
	private String fromTable;
	private String fromKey;
	private String fromColumns;

	@JsonIgnore
	private String[] fromColumsArry = new String[]{};
	
	@JsonIgnore
	public String getOpSqlForLoading(List<String> columns) {
		return String.format("select %s,%s from %s", fromKey, columns.stream().collect(Collectors.joining(",")), fromTable);
	}

	@JsonIgnore
	public String getOpSql(DatabaseBean dbBean, TableBean tableBean, String columnValue){
		return String.format("select %s, %s = 1 from %s, %s where %s.%s = %s.%s and %s.%s = %s", fromColumns, dbBean.getRowversion(), fromTable, tableBean.getTable(), fromTable, fromKey, tableBean.getTable(), toColumn, tableBean.getTable(), toColumn, columnValue);
	}

	public String getToColumn() {
		return toColumn;
	}
	public void setToColumn(String toColumn) {
		this.toColumn = toColumn;
	}
	public String getFromTable() {
		return fromTable;
	}
	public void setFromTable(String fromTable) {
		this.fromTable = fromTable;
	}
	public String getFromKey() {
		return fromKey;
	}
	public void setFromKey(String fromKey) {
		this.fromKey = fromKey;
	}
	public String getFromColumns() {
		return fromColumns;
	}
	public void setFromColumns(String fromColumns) {
		this.fromColumns = fromColumns;
		this.fromColumsArry = fromColumns.trim().split("\\s+");
	}

	public String[] getFromColumsArry() {
		return fromColumsArry;
	}

	public void doOp(DatabaseBean dbBean, TableBean tableBean, JSONObject result) throws SQLException, BeaverFatalException {
		for (String column : fromColumsArry) {
			String value = dbBean.getOpTableValue(fromTable, result.getString(toColumn), column);
			if (value != null) {
				result.put(toColumn + "_" + column, value);
			}
		}
	}

	public void doOp2(DatabaseBean dbBean, TableBean tableBean, JSONObject result) throws SQLException, BeaverFatalException {
		String opSql = getOpSql(dbBean ,tableBean, result.getString(toColumn));
		JSONArray opResult = new JSONArray();
		SqlHelper.execSqlQuery(opSql, dbBean, opResult);
		if (!opResult.isEmpty()) {
			JSONObject jObject = opResult.getJSONObject(0);
			for (String key : (Set<String>)jObject.keySet()) {
				result.put(toColumn + "_" +key, jObject.get(key));
			}
		}
	}
}
