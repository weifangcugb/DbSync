package com.cloudbeaver.client.common;

import java.sql.*;
import java.util.*;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.log4j.*;

import com.cloudbeaver.client.dbUploader.JsonAndList;
import com.cloudbeaver.client.dbbean.DatabaseBean;
import com.cloudbeaver.client.dbbean.TableBean;
import com.sun.org.apache.xalan.internal.xsltc.runtime.Hashtable;

public class SqlHelper {
    private static Logger logger = Logger.getLogger(SqlHelper.class);

    private static Hashtable conMap = new Hashtable();

    public static Connection getConn(DatabaseBean dbBean, FixedNumThreadPool threadPool) {
        if (conMap.contains(dbBean.getDb())) {
            return (Connection) conMap.get(dbBean.getDb());
        } else {
        	String driverClassName = null;
            if (dbBean.getDbUrl().startsWith("jdbc:sqlserver")) {
                driverClassName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
            } else if (dbBean.getDbUrl().startsWith("jdbc:oracle")) {
                driverClassName = "oracle.jdbc.driver.OracleDriver";
            }

            while (threadPool.isRunning()) {
                try {
    				Class.forName(driverClassName);
                    Connection conn = DriverManager.getConnection(dbBean.getDbUrl(), dbBean.getDbUserName(), dbBean.getDbPassword());
                    conMap.put(dbBean.getDb(), conn);
                    return conn;
    			} catch (ClassNotFoundException | SQLException e) {
    	            BeaverUtils.PrintStackTrace(e);
    	            logger.error("get sql connection error, msg:" + e.getMessage());
    	            
    	            BeaverUtils.sleep(3 * 1000);
    			}
			}

            return null;
        }
    }

	public static JsonAndList extractJsonAndList(DatabaseBean dbBean,TableBean tableBean, FixedNumThreadPool threadPool) throws SQLException {
		String sqlQuery = tableBean.getSqlString(dbBean.getPrison(), dbBean.getDb(), dbBean.getRowversion());
		Connection con = getConn(dbBean, threadPool);
		if (!threadPool.isRunning() || con == null) {
			return null;
		}

		PreparedStatement pStatement = con.prepareStatement(sqlQuery);
        //s.setQueryTimeout(10);
        ResultSet rs = pStatement.executeQuery();

        JSONArray array = new JSONArray();
        ArrayList<Map<String, String>> arrayList = new ArrayList<Map<String, String>>();

        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (rs.next()) {
            JSONObject jsonObj = new JSONObject();
            Map<String, String> map = new HashMap<String, String>();

            for (int i = 1; i <= columnCount; i++) {
                String columnName =metaData.getColumnLabel(i);
                String value = rs.getString(columnName);
                if (value == null) value = "";

                jsonObj.put(columnName.trim(), value.trim());
                map.put(columnName.trim(), value.trim());
            }
            array.add(jsonObj);
            arrayList.add(map);
        }

        return new JsonAndList(array.toString(), arrayList);
	}
}
