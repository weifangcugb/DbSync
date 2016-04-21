package com.cloudbeaver.client.dbbean;

import org.apache.log4j.*;
import java.util.ArrayList;

/**
 * bean for one table
 */
public class TableBean {
    private static Logger logger = Logger.getLogger(TableBean.class);

    private String table = null;
    private String xgsj = "0";
    private ArrayList<String> join = null;
    private String key = null;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getXgsj() {
        return xgsj;
    }

    public void setXgsj(String xgsj) {
        if (xgsj.indexOf('x') == -1 && xgsj.indexOf('X') == -1)
            xgsj = "0x" + xgsj;
        this.xgsj = xgsj;
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

    public String getSqlString(String prison, String dbName, String rowVersionColumn) {
        return "SELECT " + prison + " AS hdfs_prison, '" + dbName + "' AS hdfs_db, '" +
                table + "' AS hdfs_table, *" + fromClause() + whereClause(rowVersionColumn)
                + " order by " + rowVersionColumn + "limit 20";
    }

    private String fromClause() {
        if (join == null) {
            return "FROM " + table +
                    " ";
        } else {
            StringBuilder sb = new StringBuilder();
            for (String tableName : join) {
                sb.append(',');
                sb.append(tableName);
            }
            return "FROM " + table +
                    sb.toString() +
                    " ";
        }
    }

    private String whereClause(String rowVersionColumn) {
        if (join == null || key == null) {
            return "WHERE " +
                    table + "." + rowVersionColumn + " > " + xgsj + " ";
        } else {
            return "WHERE " +
                    key + " AND " +
                    table + "." + rowVersionColumn + " > " + xgsj + " ";
        }
    }
}

