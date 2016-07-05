package com.cloudbeaver.client.common;

import java.sql.*;
import java.util.ArrayList;
import java.util.Hashtable;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.log4j.*;

import com.cloudbeaver.client.dbbean.DatabaseBean;
import com.cloudbeaver.client.dbbean.TableBean;

public class SqlHelper {
    private static Logger logger = Logger.getLogger(SqlHelper.class);

    /*
     * one connection per db
     * so this class can't used in multi-thread env
     */
    private static Hashtable<String, Connection> conMap = new Hashtable<String, Connection>();

    public static Connection getConn(DatabaseBean dbBean) throws BeaverFatalException {
        if (conMap.containsKey(dbBean.getDb())) {
            return (Connection) conMap.get(dbBean.getDb());
        } else {
        	String driverClassName = null;
            if (dbBean.getDbUrl().startsWith("jdbc:sqlserver")) {
                driverClassName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
            } else if (dbBean.getDbUrl().startsWith("jdbc:oracle")) {
                driverClassName = "oracle.jdbc.driver.OracleDriver";
            } else if (dbBean.getDbUrl().startsWith("jdbc:sqlite")){
            	driverClassName = "org.sqlite.JDBC";
            }

            logger.debug(dbBean.getDb() + "," + driverClassName + "," + dbBean.getDbUrl());
            while (FixedNumThreadPool.isRunning()) {
                try {
    				Class.forName(driverClassName);
    				Connection conn = null;
    				if(dbBean.getDbUrl().startsWith("jdbc:sqlite"))
    					conn = DriverManager.getConnection(dbBean.getDbUrl());
    				else
    					conn = DriverManager.getConnection(dbBean.getDbUrl(), dbBean.getDbUserName(), dbBean.getDbPassword());
                    conMap.put(dbBean.getDb(), conn);
                    return conn;
    			} catch (ClassNotFoundException | SQLException e) {
    				conMap.remove(dbBean.getDb());
    				BeaverUtils.printLogExceptionAndSleep(e, "get sql connection error, msg:", 3 * 1000);
    			}
			}

            throw new BeaverFatalException("program get stop request from user, exit now");
        }
    }

	public static String execSqlQuery(String sqlQuery, DatabaseBean dbBean, JSONArray jArray) throws SQLException, BeaverFatalException {
		Connection con = getConn(dbBean);
		PreparedStatement pStatement = null;
		try {
//			Statement statement = con.createStatement();
//			ResultSet rs = statement.executeQuery(sqlQuery);
			pStatement = con.prepareStatement(sqlQuery);
	        //s.setQueryTimeout(10);
	        ResultSet rs = pStatement.executeQuery();

	        String maxXgsjUtilNow = CommonUploader.DB_EMPTY_ROW_VERSION;
	        ResultSetMetaData metaData = rs.getMetaData();
	        int columnCount = metaData.getColumnCount();
	        while (rs.next()) {
	            if (jArray != null) {
	                JSONObject jsonObj = new JSONObject();
	                for (int i = 1; i <= columnCount; i++) {
	                    String columnName =metaData.getColumnLabel(i);
	                    String value = rs.getString(columnName);
	                    if (value == null) value = "";

	                    jsonObj.put(columnName.trim(), value.trim());
	                }

	            	jArray.add(jsonObj);
				}

	            maxXgsjUtilNow = rs.getString(dbBean.getRowversion());
	        }

	        return maxXgsjUtilNow;
		} finally {
			if (pStatement != null) {
				try {
					pStatement.close();	
				} catch (Exception e) {
					BeaverUtils.PrintStackTrace(e);
				}
			}
		}
	}

	public static String getDBData(String prisonId, DatabaseBean dbBean,TableBean tableBean, int sqlLimitNum, JSONArray jArray) throws SQLException, BeaverFatalException {
		String sqlQuery = tableBean.getSqlString(prisonId, dbBean.getDb(), dbBean.getRowversion(), dbBean.getType(), sqlLimitNum);
		String maxRowVersion = execSqlQuery(sqlQuery, dbBean, jArray);
		if (!jArray.isEmpty() && tableBean.getJoin_subtable() != null) {
//			handle join_subtable
			for (int i = 0; i < jArray.size(); i++) {
				JSONObject jsonObject = jArray.getJSONObject(i);
				ArrayList<String> subtables = tableBean.getJoin_subtable();
				for (String subtable : subtables) {
					String subtableSql = tableBean.getSubTableSqlString(dbBean.getType(), dbBean.getRowversion(), subtable, jsonObject.getString(dbBean.getRowversion()));
					JSONArray subArray = new JSONArray();
					execSqlQuery(subtableSql, dbBean, subArray);
					if (!subArray.isEmpty()) {
						jsonObject.put(subtable, subArray);
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

	public static void removeConnection(DatabaseBean dbBean) {
		conMap.remove(dbBean.getDb());
	}
}
