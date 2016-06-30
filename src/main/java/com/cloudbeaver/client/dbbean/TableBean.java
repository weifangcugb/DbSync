package com.cloudbeaver.client.dbbean;

import org.apache.log4j.*;

import com.cloudbeaver.client.common.BeaverFatalException;
import com.cloudbeaver.client.common.CommonUploader;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.ArrayList;

/**
 * bean for one table
 */
public class TableBean {
    private static Logger logger = Logger.getLogger(TableBean.class);

    private String table;
    private String xgsj;
    private ArrayList<String> join;
    private ArrayList<String> join_subtable;
    private String key;
    private String queryTime;

    @JsonIgnore
    private String prevxgsj = CommonUploader.DB_EMPTY_ROW_VERSION;

    @JsonIgnore
    private String maxXgsj = CommonUploader.DB_EMPTY_ROW_VERSION;

//	the following two params only for YouDi system
    @JsonIgnore
    private int currentPageNum = 0;
    @JsonIgnore
    private int totalPageNum = 0;

    public String getMaxXgsj() {
		return maxXgsj;
	}

	public void setMaxXgsj(String maxXgsj) {
		this.maxXgsj = maxXgsj;
	}

	public int getTotalPageNum() {
		return totalPageNum;
	}

	public ArrayList<String> getJoin_subtable() {
		return join_subtable;
	}

	public void setJoin_subtable(ArrayList<String> join_subtable) {
		this.join_subtable = join_subtable;
	}

	public void setTotalPageNum(int totalPageNum) {
		this.totalPageNum = totalPageNum;
	}

	public int getCurrentPageNum() {
		return currentPageNum;
	}

	public void setCurrentPageNum(int currentPageNum) {
		this.currentPageNum = currentPageNum;
	}

	public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getXgsj() {
        return xgsj;
    }

    public void setXgsj(String maxXgsj) {
		prevxgsj = xgsj;
        this.xgsj = maxXgsj;
    }

    public ArrayList<String> getJoin() {
        return join;
    }

    public void setJoin(ArrayList<String> join) {
        this.join = join;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getSqlString(String prisonId, String dbName, String rowVersionColumn, String dbType, int sqlLimitNum) throws BeaverFatalException {
    	switch (dbType) {
			case CommonUploader.DB_TYPE_SQL_SERVER:
				return "SELECT top " + sqlLimitNum + " '" + prisonId + "' AS hdfs_prison, '" + dbName + "' AS hdfs_db, '" +
                table + "' AS hdfs_table, * " + fromClause() + whereClause(rowVersionColumn)
                + " order by " + table + "." + rowVersionColumn;

			case CommonUploader.DB_TYPE_SQL_SQLITE:
				return "SELECT '" + prisonId + "' AS hdfs_prison, '" + dbName + "' AS hdfs_db, '" +
                table + "' AS hdfs_table, * " + fromClause() + whereClause(rowVersionColumn)
                + " order by " + table + "." + rowVersionColumn + " limit " + sqlLimitNum;

			case CommonUploader.DB_TYPE_SQL_ORACLE:
				return "SELECT '" + prisonId + "' AS hdfs_prison, '" + dbName + "' AS hdfs_db, '" +
		                table + "' AS hdfs_table, * " + fromClause() + whereClause(rowVersionColumn, dbType, sqlLimitNum)
		                + " order by " + table + "." + rowVersionColumn;

			default:
				throw new BeaverFatalException("unknow sql type, " + dbType);
		}
        
    }

    private String whereClause(String rowVersionColumn, String dbType, int sqlLimitNum) {
		if (dbType.equals(CommonUploader.DB_TYPE_SQL_ORACLE)) {
			return whereClause(rowVersionColumn) + " and " + table + "." + rowVersionColumn + " < (" + xgsj + " + " + sqlLimitNum + ")";
		}else {
			return null;
		}
	}

	private String fromClause() {
        if (join == null) {
            return "FROM " + table + " ";
        } else {
            StringBuilder sb = new StringBuilder();
            for (String tableName : join) {
                sb.append(',').append(tableName);
            }
            return "FROM " + table + sb.toString() + " ";
        }
    }

    private String whereClause(String rowVersionColumn) {
        if (join == null || key == null) {
            return "WHERE " + table + "." + rowVersionColumn + " > " + xgsj + " ";
        } else {
            return "WHERE " + key + " AND " + table + "." + rowVersionColumn + " > " + xgsj + " ";
        }
    }

    public void setQueryTime(String touchTime){
    	this.queryTime = touchTime;
    }

	public String getQueryTime() {
		return queryTime;
	}

	public String getMaxRowVersionSqlString(String type, String rowversionColumn) {
		return "select max(" + rowversionColumn +") from " + table;
	}

	public void rollBackXgsj() {
		xgsj = prevxgsj;
	}
}

