package com.cloudbeaver.client.dbbean;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.auth0.jwt.internal.com.fasterxml.jackson.annotation.JsonIgnore;
import com.cloudbeaver.client.common.BeaverFatalException;
import com.cloudbeaver.client.common.SqlHelper;
import com.cloudbeaver.client.dbUploader.DbUploader;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

/*
 * for now, only replace operator
 */
public class TransformOp {
	private static Logger logger = Logger.getLogger(DbUploader.class);
	private static String spliter = "___";

	private String toColumn;
	private String fromTable;
	private String fromKey;
	private String fromColumns;

	@JsonIgnore
	private String[] fromColumsArry = new String[]{};
	
	@JsonIgnore
	public String getOpSqlForLoading(List<String> columns) {
		String keys = getOpKeys().stream().collect(Collectors.joining(","));
		return String.format("select %s,%s from %s", keys, columns.stream().collect(Collectors.joining(",")), fromTable);
	}

	@JsonIgnore
	public static String getSpliter() {
		return spliter;
	}

	@JsonIgnore
	public static void setSpliter(String spliter2){
		spliter = spliter2;
	}

	@JsonIgnore
	public List<String> getOpKeys(){
		return Arrays.asList(fromKey.split(spliter)).stream().map(part -> part.split("=")[0]).collect(Collectors.toList());
	}

	@JsonIgnore
	public List<String> getOpValues(){
		return Arrays.asList(fromKey.split(spliter)).stream().filter(part -> part.indexOf('=') != -1).map(part -> part.split("=")[1].trim()).collect(Collectors.toList());
	}

	@JsonIgnore
	public String getOpSql(DatabaseBean dbBean, TableBean tableBean, String columnValue){
		String selectColums = Arrays.asList(fromColumsArry).stream().collect(Collectors.joining("," + fromTable + '.', fromTable + '.', " "));
		String keyCondition = fromKey.replaceAll(spliter, " and ");
		if (tableBean.getTable().equals("TBFLOW_BASE")) {
			return String.format("select %s from %s where %s = '%s'", selectColums, fromTable, keyCondition, columnValue);
		}else{
			return String.format("select %s from %s where %s = %s", selectColums, fromTable, keyCondition, columnValue);
//			return String.format("select %s, %s.%s from %s, %s where %s.%s = %s.%s and %s.%s = %s", selectColums, tableBean.getTable(), dbBean.getRowversion(), fromTable, tableBean.getTable(), fromTable, fromKey, tableBean.getTable(), toColumn, tableBean.getTable(), toColumn, columnValue);
		}
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
			String key ="";
			if (getOpValues().size() > 0) {
				key = getOpValues().stream().collect(Collectors.joining(spliter)) + spliter;
			}
			key += result.getString(toColumn);

			String value = dbBean.getOpTableValue(fromTable, key, column);
			logger.debug("replace column, fromColumn:" + column + " toColumn:" + toColumn + " from:" 
					+ result.getString(toColumn) + " to:" + value + " key:" + key);

			if (value != null) {
				result.put(toColumn + "_" + column, value);
//				result.put(toColumn, value);
			}
		}
	}

	public void doOp2(DatabaseBean dbBean, TableBean tableBean, JSONObject result) throws SQLException, BeaverFatalException {
		String opSql = getOpSql(dbBean ,tableBean, result.getString(toColumn));
		logger.debug("query op table, sql:" + opSql);
		JSONArray opResult = new JSONArray();
		SqlHelper.execSqlQuery(opSql, dbBean, opResult);
		if (!opResult.isEmpty()) {
			JSONObject jObject = opResult.getJSONObject(0);
			for (String key : (Set<String>)jObject.keySet()) {
				result.put(toColumn + "_" +key, jObject.get(key));
//				result.put(toColumn, jObject.get(key));
			}
		}
	}

	@Override
	public String toString() {
		return "TransformOp [toColumn=" + toColumn + ", fromTable=" + fromTable + ", fromKey=" + fromKey + ", fromColumns=" + fromColumns + "]";
	}
}
