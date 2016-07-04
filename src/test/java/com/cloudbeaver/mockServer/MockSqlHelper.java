package com.cloudbeaver.mockServer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import com.cloudbeaver.client.common.BeaverFatalException;
import com.cloudbeaver.client.common.FixedNumThreadPool;
import com.cloudbeaver.client.common.SqlHelper;
import com.cloudbeaver.client.dbbean.DatabaseBean;
import com.cloudbeaver.client.dbbean.TableBean;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class MockSqlHelper extends SqlHelper{
	public static String execSqlQuery(String prisonId, DatabaseBean dbBean,TableBean tableBean, FixedNumThreadPool threadPool, int sqlLimitNum, JSONArray jArray) throws SQLException, BeaverFatalException {
		String sqlQuery = tableBean.getSqlStringForSqlite(prisonId, dbBean.getDb(), dbBean.getRowversion(), sqlLimitNum);
		
		int startpos = sqlQuery.indexOf(dbBean.getRowversion());
		int endpos = sqlQuery.indexOf("order");
		String xgsj = sqlQuery.substring(startpos+dbBean.getRowversion().length()+3, endpos-2);
		sqlQuery = sqlQuery.replace(xgsj, xgsj.substring("0x".length()));
		
		Connection con = getConn(dbBean);
		if (!threadPool.isRunning() || con == null) {
//			may be an exception is better
			return null;
		}		

		Statement statement = con.createStatement();
		ResultSet rs = statement.executeQuery(sqlQuery);

//		PreparedStatement pStatement = con.prepareStatement(sqlQuery);
//        s.setQueryTimeout(10);
//        ResultSet rs = pStatement.executeQuery();

        String maxXgsjUtilNow = tableBean.getXgsj();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (rs.next()) {
            JSONObject jsonObj = new JSONObject();

            for (int i = 1; i <= columnCount; i++) {
                String columnName =metaData.getColumnLabel(i);
                String value = rs.getString(columnName);
                if (value == null) value = "";

                jsonObj.put(columnName.trim(), value.trim());
            }
            jArray.add(jsonObj);
            String ts = rs.getString(dbBean.getRowversion());

            maxXgsjUtilNow = ts;
        }

        return maxXgsjUtilNow;
	}
}
