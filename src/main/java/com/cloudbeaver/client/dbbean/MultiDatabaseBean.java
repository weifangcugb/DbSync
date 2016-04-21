package com.cloudbeaver.client.dbbean;

import org.apache.log4j.*;

import java.util.ArrayList;
import java.util.Map;

public class MultiDatabaseBean {
	public static String FILE_UPLOAD_DB_NAME = "DocumentFiles";

    private Logger logger = Logger.getLogger(MultiDatabaseBean.class);

    private Map<String, String> conf;
    private ArrayList<DatabaseBean> databases;

    public Map<String, String> getConf() {
        return conf;
    }

    public void setConf(Map<String, String> conf) {
        this.conf = conf;
        ArrayList<DatabaseBean> dbWatchersToRemove = new ArrayList<DatabaseBean>();
        for (DatabaseBean dbWatcher : databases) {
            if (dbWatcher.db.equals(FILE_UPLOAD_DB_NAME )) {
                dbWatchersToRemove.add(dbWatcher);
            }
        }

        databases.removeAll(dbWatchersToRemove);
        for (DatabaseBean dbWatcher : databases) {
            dbWatcher.setDbExtractor(
                    conf.get("db." + dbWatcher.db + ".url"),
                    conf.get("db." + dbWatcher.db + ".username"),
                    conf.get("db." + dbWatcher.db + ".password")
            );
        }
    }

    public ArrayList<DatabaseBean> getDatabases() {
        return databases;
    }

    public void setDatabases(ArrayList<DatabaseBean> databases) {
        this.databases = databases;
    }

    public String query() {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (DatabaseBean database : databases) {
            System.out.println(database.getDb());
            logger.debug("Query database " + database.db + " .");
            String res = database.query();
            if (res.length() > 2) {
                sb.append(res).append(',');
            }
        }

        if (sb.charAt(sb.length()-1) == ',') {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append(']');
        return sb.toString();
    }
}