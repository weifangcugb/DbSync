package com.cloudbeaver.dbsync;

import org.apache.log4j.*;
import java.util.ArrayList;

/**
 * DB Watcher Bean
 * Created by gaobin on 16-4-6.
 */
public class DbWatcher {

    private static Logger logger = Logger.getLogger(DbWatcher.class);

    ArrayList<TableWatcher> tables = new ArrayList<TableWatcher>();
    String rowversion = null;
    String prison = null;
    String db = null;
    DbExtractor dbExtractor;

    public DbExtractor getDbExtractor() {
        return dbExtractor;
    }

    public void setDbExtractor(DbExtractor dbExtractor) {
        this.dbExtractor = dbExtractor;
    }

    public ArrayList<TableWatcher> getTables() {
        return tables;
    }

    public void setTables(ArrayList<TableWatcher> tables) {
        this.tables = tables;
        for (TableWatcher tableWatcher : tables) {
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

    public DbWatcher() {
        logger.debug("Created DbWatcher");
    }

    public String query() {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (TableWatcher t : tables) {
            logger.debug("Executing query : " + t.sql());
            //String res = dbExtractor.extractJson(t.sql());
            JsonAndList jsonAndList = dbExtractor.extractJsonAndList(t.sql(),
                    java.util.Arrays.asList("max_" + t.getRowversion()));
            if (jsonAndList == null) continue;
            String res = jsonAndList.getJson();
            if (!res.equals("[]")) {
                sb.append(res.substring(1, res.length()-1))
                        .append(',');
                        //.append(',').append('\n');
                t.setXgsj(jsonAndList.getList().get(0).get("max_" + t.getRowversion()));
            }
        }
        if (sb.charAt(sb.length()-1) == ',') {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append(']');
        return sb.toString();
    }

}
