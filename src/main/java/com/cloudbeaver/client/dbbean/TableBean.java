package com.cloudbeaver.client.dbbean;

import org.apache.log4j.*;
import java.util.ArrayList;

/**
 * Table Bean
 */
public class TableBean {

    private static Logger logger = Logger.getLogger(TableBean.class);

    private DatabaseBean dbWatcher = null;
    private String table = null;
    private String rowversion = null;
    private String xgsj = "0";
    private ArrayList<String> join = null;
    private String key = null;

    public DatabaseBean getDbWatcher() {
        return dbWatcher;
    }

    public void setDbWatcher(DatabaseBean dbWatcher) {
        this.dbWatcher = dbWatcher;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }


    public String getRowversion() {
        if (rowversion == null) rowversion = dbWatcher.rowversion;
        return rowversion;
    }

    public void setRowversion(String rowversion) {
        this.rowversion = rowversion;
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

    public String sql() {
        return "SELECT " + dbWatcher.prison + " AS hdfs_prison, '" +
                dbWatcher.db + "' AS hdfs_db, '" +
                table + "' AS hdfs_table, *, max_" + getRowversion() + " " +
                fromClause() +
                ", (SELECT MAX(" + getRowversion() + ") AS max_" + getRowversion() + " FROM " + table + ") AS Xesx " +
                whereClause();
    }

    public String sqlWithoutRowversion() {
        return "SELECT " + dbWatcher.prison + " AS hdfs_prison, '" +
                dbWatcher.db + "' AS hdfs_db, '" +
                table + "' AS hdfs_table, * " +
                fromClause() + whereClause();
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

    private String whereClause() {
        if (join == null || key == null) {
            return "WHERE " +
                    table + "." + rowversionColumn() + " > " + xgsj + " ";
        } else {
            return "WHERE " +
                    key + " AND " +
                    table + "." + rowversionColumn() + " > " + xgsj + " ";
        }
    }

    private String rowversionColumn() {
        if (rowversion == null) {
            return dbWatcher.rowversion;
        } else {
            return rowversion;
        }
    }
}

