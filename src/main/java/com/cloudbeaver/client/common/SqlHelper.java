package com.cloudbeaver.client.common;

import java.sql.*;
import java.util.Hashtable;
import java.util.List;
import java.util.stream.Collectors;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.log4j.*;

import com.cloudbeaver.client.dbbean.DatabaseBean;
import com.cloudbeaver.client.dbbean.TableBean;

public class SqlHelper {
    private static Logger logger = Logger.getLogger(SqlHelper.class);

    /*
     * one connection per db, and cache these connection in map
     * so this class can't used in multi-thread env
     */
    private static Hashtable<String, Connection> conMap = new Hashtable<String, Connection>();

    public static Connection getCachedConnKeepTrying(DatabaseBean dbBean) throws BeaverFatalException {
            while (FixedNumThreadPool.isRunning()) {
            	try{
                    if (!conMap.containsKey(dbBean.getDb())) {
                    	conMap.put(dbBean.getDb(), getDBConnection(dbBean));
                    }

                    return (Connection) conMap.get(dbBean.getDb());
            	}catch (SQLException | ClassNotFoundException e) {
            		BeaverUtils.printLogExceptionAndSleep(e, "can't get connection", 5000);
				}
			}

            throw new BeaverFatalException("program get stop request from user, exit now");
    }

    public static synchronized Connection getDBConnection(DatabaseBean dbBean) throws ClassNotFoundException, SQLException{
    	String driverClassName = null;
        if (dbBean.getDbUrl().startsWith("jdbc:sqlserver")) {
            driverClassName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        } else if (dbBean.getDbUrl().startsWith("jdbc:oracle")) {
            driverClassName = "oracle.jdbc.driver.OracleDriver";
        } else if (dbBean.getDbUrl().startsWith("jdbc:sqlite")){
        	driverClassName = "org.sqlite.JDBC";
        } else if (dbBean.getDbUrl().startsWith("jdbc:postgresql")){
        	driverClassName = "org.postgresql.Driver";
        }

        logger.debug(dbBean.getDb() + "," + driverClassName + "," + dbBean.getDbUrl());

        Connection conn = null;
		Class.forName(driverClassName);
		if (dbBean.getDbUrl().startsWith("jdbc:sqlite")) {
			conn = DriverManager.getConnection(dbBean.getDbUrl());
		} else {
			conn = DriverManager.getConnection(dbBean.getDbUrl(), dbBean.getDbUserName(), dbBean.getDbPassword());
		}
		return conn;
    }

    public static JSONArray execSimpleQuery(String sqlQuery, DatabaseBean dbBean, Connection con) throws ClassNotFoundException, SQLException{
    	PreparedStatement pStatement = con.prepareStatement(sqlQuery);
    	ResultSet rs = pStatement.executeQuery();

    	JSONArray resultArray = new JSONArray();
    	while (rs.next()) {
    		JSONObject jsonObj = new JSONObject();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            for (int i = 1; i <= columnCount; i++) {
                String columnName =metaData.getColumnLabel(i).trim();
                String value = rs.getString(columnName);
                jsonObj.put(columnName, value == null ? "" : value.trim());
            }

            resultArray.add(jsonObj);
		}

    	return resultArray;
    }

	private static String execSqlQuery(String sqlQuery, DatabaseBean dbBean, JSONArray jArray) throws SQLException, BeaverFatalException {
		logger.debug("sql:" + sqlQuery);
		Connection con = getCachedConnKeepTrying(dbBean);
		try {
//			Statement statement = con.createStatement();
//			ResultSet rs = statement.executeQuery(sqlQuery);
			PreparedStatement pStatement = con.prepareStatement(sqlQuery);
	        //s.setQueryTimeout(10);
	        ResultSet rs = pStatement.executeQuery();

	        String maxXgsjUtilNow = CommonUploader.DB_EMPTY_ROW_VERSION;
	        ResultSetMetaData metaData = rs.getMetaData();
	        int columnCount = metaData.getColumnCount();
	        while (rs.next()) {
	            if (jArray != null) {
	                JSONObject jsonObj = new JSONObject();
	                boolean firstRowVersionColumn = true;
	                for (int i = 1; i <= columnCount; i++) {
	                    String columnName =metaData.getColumnLabel(i);

	                    if (firstRowVersionColumn && columnName.equals(dbBean.getRowversion())) {
							maxXgsjUtilNow = rs.getString(columnName.trim());
							firstRowVersionColumn = false;
						}

	                    String value = rs.getString(columnName.trim());
	                    if (value == null) value = "";

	                    jsonObj.put(columnName.trim(), value.trim());
	                }

	                jsonObj.put(dbBean.getRowversion(), maxXgsjUtilNow);
	            	jArray.add(jsonObj);
				}

	            String maxV = rs.getString(dbBean.getRowversion());
	            if (maxV != null && maxV.length() > 0) {
	            	maxXgsjUtilNow = maxV;
				}
	        }
	        rs.close();
	        pStatement.clearBatch();
	        pStatement.close();

	        return maxXgsjUtilNow;
		} catch(SQLException e) {
			removeCachedConnection(dbBean);
			throw e;
		}
	}

	public static String getDBData(String prisonId, DatabaseBean dbBean,TableBean tableBean, int sqlLimitNum, JSONArray jArray) throws SQLException, BeaverFatalException {
		String sqlQuery = tableBean.getSqlString(dbBean.getRowversion(), dbBean.getType(), sqlLimitNum);
		String maxRowVersion = execSqlQuery(sqlQuery, dbBean, jArray);
		if (!jArray.isEmpty()) {
//			handle join_subtable
			for (int i = 0; i < jArray.size(); i++) {
				JSONObject jsonObj = jArray.getJSONObject(i);

                jsonObj.put("hdfs_prison", prisonId);
                jsonObj.put("hdfs_db", dbBean.getDb());
                jsonObj.element("hdfs_table", tableBean.getTable());

                if (tableBean.getJoin_subtable() != null) {
    				List<String> subtables = tableBean.getJoin_subtable();
					String subtableSql = tableBean.getSubTableSqlString(dbBean, tableBean, subtables, jsonObj.getString(dbBean.getRowversion()));
					JSONArray subArray = new JSONArray();
					execSqlQuery(subtableSql, dbBean, subArray);
					if (!subArray.isEmpty()) {
						jsonObj.put(subtables.stream().collect(Collectors.joining("_")), subArray);
					}
				}
			}
		}
		return maxRowVersion;
	}

	public static String getMaxRowVersion(DatabaseBean dbBean, TableBean tableBean) throws SQLException, BeaverFatalException{
		String sqlQuery = tableBean.getMaxRowVersionSqlString(dbBean.getType(), dbBean.getRowversion());
		return execSqlQuery(sqlQuery, dbBean, null);
	}

	public static void removeCachedConnection(DatabaseBean dBean) {
		closeConnection(conMap.get(dBean.getDb()));
		conMap.remove(dBean.getDb());
	}

	public static void closeConnection(Connection conn) {
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				BeaverUtils.printLogExceptionWithoutSleep(e, "can't close connection");
			}
		}
	}
}
