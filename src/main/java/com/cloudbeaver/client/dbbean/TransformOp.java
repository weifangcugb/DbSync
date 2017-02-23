package com.cloudbeaver.client.dbbean;

import com.auth0.jwt.internal.com.fasterxml.jackson.annotation.JsonIgnore;

public class TransformOp {
	private String toColumn;
	private String fromTable;
	private String fromKey;
	private String fromColumns;

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
	}
}
