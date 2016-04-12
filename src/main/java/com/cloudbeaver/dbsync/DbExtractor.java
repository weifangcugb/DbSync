package com.cloudbeaver.dbsync;

import java.sql.*;
import java.util.*;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.log4j.*;

/**
 * Created by gaobin on 16-3-31.
 */
public class DbExtractor {

    private static Logger logger = Logger.getLogger(DbExtractor.class);

    String url;
    String username;
    String password;
    Connection conn;
    String driverClassName = null;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public DbExtractor (String url, String username, String password) {
        setUrl(url);
        setUsername(username);
        setPassword(password);
        getConn(url, username, password);
    }

    protected Connection getConn () {
        boolean connClosed = false;
        try {
            if (conn == null) {
                connClosed = true;
            } else {
                connClosed = conn.isClosed();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if (connClosed) {
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
                logger.error(ex.getMessage());
                //System.exit(1);
            }
        }
        return conn;
    }

    @Deprecated
    protected Connection getConn (String url, String username, String password) {
        setUrl(url);
        setUsername(username);
        setPassword(password);
        return getConn();
    }

    private static String resultSetToJson(ResultSet rs) throws SQLException
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
                if (value == null) value = "";
                jsonObj.put(columnName.trim(), value.trim());
            }
            array.add(jsonObj);
        }
        return array.toString();
    }

    private static List<Map<String, String>> resultSetToList(ResultSet rs) throws SQLException
    {
        ArrayList<Map<String, String>> arrayList = new ArrayList<Map<String, String>>();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (rs.next()) {
            Map<String, String> map = new HashMap<String, String>();

            // 遍历每一列
            for (int i = 1; i <= columnCount; i++) {
                String columnName =metaData.getColumnLabel(i);
                String value = rs.getString(columnName);
                if (value == null) value = "";
                map.put(columnName.trim(), value.trim());
            }
            arrayList.add(map);
        }
        return arrayList;
    }

    private static JsonAndList resultSetToJsonAndList(ResultSet rs, Collection<String> excludedColumns) throws SQLException
    {
        JSONArray array = new JSONArray();
        ArrayList<Map<String, String>> arrayList = new ArrayList<Map<String, String>>();

        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        // 遍历ResultSet中的每条数据
        while (rs.next()) {
            JSONObject jsonObj = new JSONObject();
            Map<String, String> map = new HashMap<String, String>();

            // 遍历每一列
            for (int i = 1; i <= columnCount; i++) {
                String columnName =metaData.getColumnLabel(i);
                String value = rs.getString(columnName);
                if (value == null) value = "";
                if (! excludedColumns.contains(columnName)) {
                    jsonObj.put(columnName.trim(), value.trim());
                } else {
                    map.put(columnName.trim(), value.trim());
                }
            }
            array.add(jsonObj);
            arrayList.add(map);
        }
        return new JsonAndList(array.toString(), arrayList);
    }

    public String extract(PreparedStatement s) {
        return extractJson(s);
    }

    public String extract(String statement) {
        return extractJson(statement);
    }

    public String extractJson(PreparedStatement s) {
        String json = "[]";
        ResultSet rs = null;
        try {
            rs = s.executeQuery();
            json = this.resultSetToJson(rs);
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            //System.exit(1);
            getConn();
        }
        return json;
    }

    public String extractJson(String statement) {
        PreparedStatement s = null;
        try {
            s = getConn().prepareStatement(statement);
            s.setQueryTimeout(10);
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            //System.exit(1);
        }
        return extractJson(s);
    }

    public List<Map<String, String>> extractList(PreparedStatement s) {
        List<Map<String, String>> list = null;
        ResultSet rs = null;
        try {
            rs = s.executeQuery();
            list = this.resultSetToList(rs);
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            //System.exit(1);
            getConn();
        }
        return list;
    }

    public List<Map<String, String>> extractList(String statement) {
        PreparedStatement s = null;
        try {
            s = getConn().prepareStatement(statement);
            s.setQueryTimeout(10);
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            //System.exit(1);
        }
        return extractList(s);
    }

    public JsonAndList extractJsonAndList(PreparedStatement s, Collection<String> excludedColumns) {
        JsonAndList jsonAndList = null;
        ResultSet rs = null;
        try {
            s.setQueryTimeout(10);
            System.out.println(s.getQueryTimeout());
            rs = s.executeQuery();
            jsonAndList = this.resultSetToJsonAndList(rs, excludedColumns);
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("SQL ERROR: 错误原因: " + e.getMessage());
            //System.exit(1);
            getConn();
        }
        return jsonAndList;
    }

    public JsonAndList extractJsonAndList(String statement, Collection<String> excludedColumns) {
        PreparedStatement s = null;
        try {
            s = getConn().prepareStatement(statement);
            s.setQueryTimeout(10);
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            //System.exit(1);
        }
        return extractJsonAndList(s, excludedColumns);
    }

    public void setDriverClassName (String driverClassName) {
        this.driverClassName = driverClassName;
    }

    public static void main(String[] args) {
        DbExtractor ya = new DbExtractor("jdbc:sqlserver://192.168.1.107;databaseName=st1417", "gaobin", "abc#123");
        PreparedStatement preparedStatement = null;
        try {
            ya.conn = ya.getConn();
            preparedStatement = ya.conn.prepareStatement("SELECT TOP 1000 [xm]\n" +
                    "      ,[nl]\n" +
                    "      ,[sr]\n" +
                    "      ,[rw]\n" +
                    "  FROM [st1417].[dbo].[ycjy]\n" +
                    "  where rw >= 0x00000000000007D3");
            preparedStatement.setQueryTimeout(10);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println(ya.extractJson(preparedStatement));
        //System.out.println(ya);
    }
}
