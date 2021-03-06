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
    private String flowid;
    @JsonProperty("create_time")
    private String createTime;
    @JsonProperty("RID")
    private String rid;

	private List<TransformOp> replaceOp = new ArrayList<TransformOp>();

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

	private String ORACLE_DATA_FORMAT = "yyyymmddhh24miss";

	public List<TransformOp> getReplaceOp() {
		return replaceOp;
	}

	public void setReplaceOp(List<TransformOp> replaceOp) {
		this.replaceOp = replaceOp;
	}

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

	public String getRid() {
		return rid;
	}

	public void setRid(String rid) {
		this.rid = rid;
		xgsj = rid;
	}

    public String getFlowid() {
		return flowid;
	}

	public void setFlowid(String flowid) {
		this.flowid = flowid;
		xgsj = flowid;
	}

	public String getCreateTime() {
		return createTime;
	}

	public void setCreateTime(String createTime) {
		this.createTime = createTime;
		xgsj = createTime;
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

    public void setXgsj(String nowXgsj) {
    	logger.debug("set xgsj, before:" + xgsj + " after:" + nowXgsj);
		prevxgsj = xgsj;
        this.xgsj = nowXgsj;
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
			case CommonUploader.DB_TYPE_MYSQL:
				return "SELECT " + selectColumnClause() + fromClause() + whereClause(rowVersionColumn, dbType, sqlLimitNum)
		                + " order by " + table + "." + rowVersionColumn;
			default:
				throw new BeaverFatalException("unknow sql type, " + dbType);
		}
    }

	public String getSubTableSqlString(DatabaseBean dbBean, TableBean tableBean, List<String> subtables, List<String> subKeys) throws BeaverFatalException {
		String tables = subtables.stream().collect(Collectors.joining(".*,", "", ".*"));
		String from = subtables.stream().collect(Collectors.joining(",", tableBean.getTable() + ',', " "));
		if (isXFZXFlowTable(dbBean.getType())) {
//			hack here
			return "select " + tables + " from " + from + " where " + tableBean.getKey() 
				+ " and " + tableBean.getTable() + ".flowdraftid in " + subKeys.stream().map(key -> "'"+key+"'").collect(Collectors.joining(",","(",")"));
		}else if (isXFZXDateSystem(dbBean.getType(), dbBean.getRowversion())) {
			return "select " + tables + " from " + from + " where " + tableBean.getKey() 
				+ " and to_char(" + tableBean.getTable() + "." + dbBean.getRowversion() + ", '" + ORACLE_DATA_FORMAT + "') in " 
				+ subKeys.stream().map(key -> "'"+key+"'").collect(Collectors.joining(",","(",")"));
		}else{
			return "select " + tables + " from " + from + " where " + tableBean.getKey() 
				+ " and " + tableBean.getTable() + "." + dbBean.getRowversion() + " in " + subKeys.stream().collect(Collectors.joining(",", "(", ")"));
		}
	}

    private String whereClause(String rowVersionColumn, String dbType, int sqlLimitNum) {
//    	only used by oracle or mysql
		if (dbType.equals(CommonUploader.DB_TYPE_SQL_ORACLE) || dbType.equals(CommonUploader.DB_TYPE_MYSQL)) {
			if (isXFZXFlowTable(dbType)) {
				return String.format(" WHERE %s to_number(to_char(%s.%s, '" + ORACLE_DATA_FORMAT + "')) > %s AND to_number(to_char(%s.%s, '" + ORACLE_DATA_FORMAT + "')) <= %s", 
						(join !=null && key != null)? key + " AND ":"", table, rowVersionColumn, xgsj, table, rowVersionColumn, SqlHelper.nextOracleDateTime(xgsj, sqlLimitNum));
			}else if (isXFZXDateSystem(dbType, rowVersionColumn)) {
//				xfzx system
//				return String.format("WHERE %s %s.%s > '%s' AND %s.%s <= '%s'", 
//						(join !=null && key != null)? key + " AND ":"", table, rowVersionColumn, xgsj, table, rowVersionColumn, SqlHelper.nextOracleDateTime(xgsj, sqlLimitNum));
				return String.format(" WHERE %s to_number(to_char(%s.%s, '" + ORACLE_DATA_FORMAT + "')) > %s AND to_number(to_char(%s.%s, '" + ORACLE_DATA_FORMAT + "')) <= %s", 
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
			return "select max(to_number(to_char(" + rowversionColumn +",'" + ORACLE_DATA_FORMAT + "'))) as " + rowversionColumn + " from " + table;
		}
		else{
			return "select max(" + rowversionColumn +") as " + rowversionColumn + " from " + table;
		}
	}

	public String getMinRowVersionSqlString(String type, String rowversionColumn) {
		String columnExp = rowversionColumn;
		if (isXFZXDateSystem(type, rowversionColumn)) {/*hack here*/
			columnExp = "to_number(to_char(" + rowversionColumn + ", '" + ORACLE_DATA_FORMAT  + "'))";
		}

		return String.format("select min(%s) as %s from %s where %s > %s", columnExp, rowversionColumn, table, columnExp, xgsj);
	}

	public boolean isXFZXFlowTable(String type) {
		return (type.equals(CommonUploader.DB_TYPE_SQL_ORACLE) && (table.equals("TBFLOW_BASE")));
	}

	@JsonIgnore
	public long getXgsjAsLong() {
		return Long.parseLong(xgsj);
	}

	@JsonIgnore
	public long getMaxXgsjAsLong() {
		return Long.parseLong(maxXgsj);
	}

	@JsonIgnore
	public double getMaxXgsjAsDouble() {
		return Double.parseDouble(maxXgsj);
	}
	
	@JsonIgnore
	public double getXgsjAsDouble() {
		return Double.parseDouble(xgsj);
	}
}

