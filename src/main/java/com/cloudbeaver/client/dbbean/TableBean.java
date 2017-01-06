package com.cloudbeaver.client.dbbean;

import org.apache.log4j.*;

import com.cloudbeaver.client.common.BeaverFatalException;
import com.cloudbeaver.client.common.BeaverTableIsFullException;
import com.cloudbeaver.client.common.CommonUploader;
import com.cloudbeaver.client.common.SqlHelper;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * bean for one table
 */
public class TableBean implements Serializable{
    private static Logger logger = Logger.getLogger(TableBean.class);

    private String table;
    private String xgsj = "0";
    private ArrayList<String> join;
    private ArrayList<String> join_subtable;
    private String key;
    private String starttime = "0";
    @JsonProperty("ID")
    private String id = "0";
    @JsonProperty("OPTIME")
    private String opTime;/*xfzx*/
    @JsonProperty("MDATE")
    private String mDate;/*xfzx*/
    @JsonProperty("FLOWSN")
    private String flowSn;/*flowsn*/

	@JsonIgnore
    private String queryTime;

    @JsonIgnore
    private String prevxgsj = CommonUploader.DB_EMPTY_ROW_VERSION;

    @JsonIgnore
    private String maxXgsj = CommonUploader.DB_EMPTY_ROW_VERSION;

//	the following two params only for YouDi system
    @JsonIgnore
    private int prevPageNum = 0;
    @JsonIgnore
    private int currentPageNum = 0;
    @JsonIgnore
    private int prevTotalPageNum = 0;
    @JsonIgnore
    private int totalPageNum = 0;

    @JsonIgnore
    private boolean syncTypeOnceADay = false;

	public String getStarttime() {
		return starttime;
	}

	public void setStarttime(String starttime) {
		this.starttime = starttime;
		xgsj = starttime;
	}

    public String getOpTime() {
		return opTime;
	}

	public void setOpTime(String opTime) {
		this.opTime = opTime;
		xgsj = opTime;
	}

	public String getmDate() {
		return mDate;
	}

	public void setmDate(String mDate) {
		this.mDate = mDate;
		logger.info("mdate:" + mDate + " xgsj:" + xgsj);
		xgsj = mDate;
	}

	public String getFlowSn() {
		return flowSn;
	}

	public void setFlowSn(String flowSn) {
		xgsj = flowSn;
		this.flowSn = flowSn;
	}

	public void setID(String ID) {
		this.id = ID;
		xgsj = ID;
	}

	//for test
	public String getID() {
		return id;
	}

	public void moveToNextXgsj(int interval) throws BeaverTableIsFullException {
		setXgsjByInterval(interval);
	}

	@JsonIgnore
	public void setXgsjByInterval(int interval) {
		prevxgsj = xgsj;
		prevPageNum = currentPageNum;
		prevTotalPageNum = totalPageNum;

		long timestamp = getXgsjAsLong() + interval;
		xgsj = "" + timestamp;
		currentPageNum = 0;
		totalPageNum = 0;
	}

	@JsonIgnore
	public void setXgsjwithLong(long timestamp) {
		prevxgsj = "" + timestamp;
		xgsj = "" + timestamp;
		prevPageNum = currentPageNum;
		prevTotalPageNum = totalPageNum;
		currentPageNum = 0;
		totalPageNum = 0;
	}

	public void rollBackXgsj() {
		currentPageNum = prevPageNum;
		totalPageNum = prevTotalPageNum;
		xgsj = prevxgsj;
	}

    public String getMaxXgsj() {
		return maxXgsj;
	}

	public void setMaxXgsj(String maxXgsj) {
		this.maxXgsj = maxXgsj;
	}

	public ArrayList<String> getJoin_subtable() {
		return join_subtable;
	}

	public void setJoin_subtable(ArrayList<String> join_subtable) {
		this.join_subtable = join_subtable;
	}

	public String getPrevxgsj() {
		return prevxgsj;
	}

	@JsonIgnore
	public long getPrevxgsjAsLong() {
		return Long.parseLong(prevxgsj);
	}

	public void setPrevxgsj(String prevxgsj) {
		this.prevxgsj = prevxgsj;
	}

	public boolean isSyncTypeOnceADay() {
		return syncTypeOnceADay;
	}

	public void setSyncTypeOnceADay(boolean syncTypeOnceADay) {
		this.syncTypeOnceADay = syncTypeOnceADay;
	}

	public int getPrevPageNum() {
		return prevPageNum;
	}

	public void setPrevPageNum(int prevPageNum) {
		this.prevPageNum = prevPageNum;
	}

	public int getPrevTotalPageNum() {
		return prevTotalPageNum;
	}

	public void setPrevTotalPageNum(int prevTotalPageNum) {
		this.prevTotalPageNum = prevTotalPageNum;
	}

	public int getTotalPageNum() {
		return totalPageNum;
	}

	public void setTotalPageNum(int totalPageNum) {
		this.prevTotalPageNum = this.totalPageNum;
		this.totalPageNum = totalPageNum;
	}

	public int getCurrentPageNum() {
		return currentPageNum;
	}

	public void setCurrentPageNum(int currentPageNum) {
		this.prevPageNum = this.currentPageNum;
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

    public String getSqlString(String rowVersionColumn, String dbType, int sqlLimitNum) throws BeaverFatalException {
    	switch (dbType) {
			case CommonUploader.DB_TYPE_SQL_SERVER:
				return "SELECT top " + sqlLimitNum  + " " + table + "." + rowVersionColumn + " as " + rowVersionColumn + ", * " + fromClause() + whereClause(rowVersionColumn)
                		+ " order by " + table + "." + rowVersionColumn;

			case CommonUploader.DB_TYPE_SQL_SQLITE:
				return "SELECT * " + fromClause() + whereClause(rowVersionColumn)
                		+ " order by " + table + "." + rowVersionColumn + " limit " + sqlLimitNum;

			case CommonUploader.DB_TYPE_SQL_ORACLE:
				return "SELECT " + selectColumnClause() + fromClause() + whereClause(rowVersionColumn, dbType, sqlLimitNum)
		                + " order by " + table + "." + rowVersionColumn;
			default:
				throw new BeaverFatalException("unknow sql type, " + dbType);
		}
    }

	public String getSubTableSqlString(DatabaseBean dbBean, TableBean tableBean, List<String> subtables, String versionOffset) throws BeaverFatalException {
		String tables = subtables.stream().collect(Collectors.joining(".*,", "", ".*"));
		String from = subtables.stream().collect(Collectors.joining(",", tableBean.getTable() + ',', " "));
		if (isXFZXDateSystem(dbBean.getType(), dbBean.getRowversion())) {
			return "select " + tables + " from " + from + " where " + tableBean.getKey() 
			+ " and to_char(" + tableBean.getTable() + "." + dbBean.getRowversion() + ") =" + versionOffset;
		}else{
			return "select " + tables + " from " + from + " where " + tableBean.getKey() 
			+ " and " + tableBean.getTable() + "." + dbBean.getRowversion() + "=" + versionOffset;
		}
	}

    private String whereClause(String rowVersionColumn, String dbType, int sqlLimitNum) {
//    	only used by oracle
		if (dbType.equals(CommonUploader.DB_TYPE_SQL_ORACLE)) {
			if (isXFZXDateSystem(dbType, rowVersionColumn)) {
//				xfzx system
				return String.format("WHERE %s to_number(to_char(%s.%s, 'yyyymmddhh24mmss')) > %s AND to_number(to_char(%s.%s, 'yyyymmddhh24mmss')) <= %s", 
						(join !=null && key != null)? key + " AND ":"", table, rowVersionColumn, xgsj, table, rowVersionColumn, SqlHelper.nextOracleDateTime(xgsj, sqlLimitNum));
			}else{
				return whereClause(rowVersionColumn) + " and " + table + "." + rowVersionColumn + " <= (" + xgsj + " + " + sqlLimitNum + ")";
			}
		}else {
			return null;
		}
	}

	private String selectColumnClause() {
    	StringBuilder sb = new StringBuilder();
    	sb.append(table + ".* ");
		if (join != null) {
			for (String joinName : join) {
				sb.append(',').append(joinName).append(".* ");
			}
		}
		return sb.toString();
	}

	private String fromClause() {
        if (join == null) {
            return "FROM " + table + " ";
        } else {
            StringBuilder sb = new StringBuilder();
            for (String tableName : join) {
                sb.append(',').append(tableName);
            }
            return " FROM " + table + sb.toString() + " ";
        }
    }

    private String whereClause(String rowVersionColumn) {
        if (join == null || key == null) {
            return "WHERE " + table + "." + rowVersionColumn + " > " + xgsj + " ";
        } else {
            return "WHERE " + key + " AND " + table + "." + rowVersionColumn + " > " + xgsj + " ";
        }
    }

    private boolean isXFZXDateSystem(String type, String rowversionColumn){
    	return (type.equals(CommonUploader.DB_TYPE_SQL_ORACLE) && (rowversionColumn.equals("OPTIME") || rowversionColumn.equals("MDATE")));
    }

    public void setQueryTime(String touchTime){
    	this.queryTime = touchTime;
    }

	public String getQueryTime() {
		return queryTime;
	}

	public String getMaxRowVersionSqlString(String type, String rowversionColumn) {
		if (isXFZXDateSystem(type, rowversionColumn)) {/*hack here*/
			return "select max(to_number(to_char(" + rowversionColumn +",'yyyymmddhhmmss'))) as " + rowversionColumn + " from " + table;
		}else{
			return "select max(" + rowversionColumn +") as " + rowversionColumn + " from " + table;
		}
	}

	@JsonIgnore
	public long getXgsjAsLong() {
		return Long.parseLong(xgsj);
	}

	@JsonIgnore
	public long getMaxXgsjAsLong() {
		return Long.parseLong(maxXgsj);
	}
}

