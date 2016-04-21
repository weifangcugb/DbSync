package com.cloudbeaver.client.dbbean;

import org.apache.log4j.*;

import com.cloudbeaver.client.dbUploader.DbExtractor;
import com.cloudbeaver.client.dbUploader.JsonAndList;

import java.util.ArrayList;

/**
 * DB Watcher Bean
 */
public class DatabaseBean {

    private static Logger logger = Logger.getLogger(DatabaseBean.class);

    ArrayList<TableBean> tables = new ArrayList<TableBean>();
    String rowversion = null;
    String prison = null;
    String db = null;
    DbExtractor dbExtractor;

    public String toString(){
    	return db + "," + prison + "," + rowversion;
    }

    public DbExtractor getDbExtractor() {
        return dbExtractor;
    }

    public void setDbExtractor(DbExtractor dbExtractor) {
        this.dbExtractor = dbExtractor;
    }

    public void setDbExtractor(String url, String username, String password) {
        this.setDbExtractor(new DbExtractor(
                url, username, password
        ));
    }

    public ArrayList<TableBean> getTables() {
        return tables;
    }

    public void setTables(ArrayList<TableBean> tables) {
        this.tables = tables;
        for (TableBean tableWatcher : tables) {
            tableWatcher.setDbWatcher(this);
        }
    }

    public String getRowversion() {
        return rowversion;
    }

    public void setRowversion(String rowversion) {
        this.rowversion = rowversion;
    }

    public String getPrison() {
        return prison;
    }

    public void setPrison(String prison) {
        this.prison = prison;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String query() {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (TableBean t : tables) {
            logger.debug("Executing query : " + t.sql());
            //String res = dbExtractor.extractJson(t.sql());
            JsonAndList jsonAndList = dbExtractor.extractJsonAndList(t.sql(),
                    java.util.Arrays.asList("max_" + t.getRowversion()));
            if (jsonAndList == null) continue;
            String res = jsonAndList.getJson();
            if (res.length() > 2) {
                sb.append(res.substring(1, res.length()-1))
                        .append(',');
                        //.append(',').append('\n');
            }
            if (jsonAndList != null &&
                    jsonAndList.getList() != null &&
                    jsonAndList.getList().size() > 0) {
                t.setXgsj(jsonAndList.getList().get(0).get("max_" + t.getRowversion()));
            }
        }
        if (sb.charAt(sb.length() - 1) == ',') {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append(']');
        return sb.toString();
    }

}
