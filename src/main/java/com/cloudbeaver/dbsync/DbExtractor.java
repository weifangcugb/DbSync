package com.cloudbeaver.dbsync;

import net.sf.json.JSONArray;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import net.sf.json.JSONArray;
import com.microsoft.sqlserver.jdbc.*;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import org.apache.log4j.*;

/**
 * Created by gaobin on 16-3-31.
 */
public class DbExtractor {

    private static Logger logger = Logger.getLogger(DbExtractor.class);

    Connection conn;
    String driverClassName = null;

    public DbExtractor (String url, String username, String password) {
        getConn(url, username, password);
    }

    protected Connection getConn (String url, String username, String password) {
        if (conn == null) {
            if (driverClassName == null) {
                if (url.startsWith("jdbc:sqlserver")) {
                    driverClassName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
                } else if (url.startsWith("jdbc:oracle")) {
                    driverClassName = "oracle.jdbc.driver.OracleDriver";
                }
            }
            try {
                Class.forName(driverClassName);
                conn = DriverManager.getConnection(url, username, password);
            } catch (Exception ex) {
                ex.printStackTrace();
                logger.fatal(ex.getMessage());
                System.exit(1);
            }
        }
        return conn;
    }

    public static String resultSetToJson(ResultSet rs) throws SQLException
    {
        // json数组
        JSONArray array = new JSONArray();
        // 获取列数
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        // 遍历ResultSet中的每条数据
        while (rs.next()) {
            JSONObject jsonObj = new JSONObject();

            // 遍历每一列
            for (int i = 1; i <= columnCount; i++) {
                String columnName =metaData.getColumnLabel(i);
                String value = rs.getString(columnName);
                jsonObj.put(columnName.trim(), value.trim());
            }
            array.add(jsonObj);
        }
        return array.toString();
    }

    public String extract(PreparedStatement s) {
        String json = "[]";
        ResultSet rs = null;
        try {
            rs = s.executeQuery();
            json = this.resultSetToJson(rs);
        } catch (SQLException e) {
            e.printStackTrace();
            logger.fatal(e.getMessage());
            System.exit(1);
        }
        return json;
    }

    public String extract(String statement) {
        if (conn != null) {
            PreparedStatement s = null;
            try {
                s = conn.prepareStatement(statement);
            } catch (SQLException e) {
                e.printStackTrace();
                logger.fatal(e.getMessage());
                System.exit(1);
            }
            return extract(s);
        } else {
            System.out.println("NO Conn Exception");
            logger.fatal("NO Conn Exception");
            System.exit(1);
            return null;
        }
    }

    public void setDriverClassName (String driverClassName) {
        this.driverClassName = driverClassName;
    }

    public static void main(String[] args) {
        DbExtractor ya = new DbExtractor("jdbc:sqlserver://127.0.0.1;databaseName=st1417", "gaobin", "abc#123");
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = ya.conn.prepareStatement("SELECT TOP 1000 [xm]\n" +
                    "      ,[nl]\n" +
                    "      ,[sr]\n" +
                    "      ,[rw]\n" +
                    "  FROM [st1417].[dbo].[ycjy]\n" +
                    "  where rw >= 0x00000000000007D3");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println(ya.extract(preparedStatement));
        //System.out.println(ya);
    }
}
