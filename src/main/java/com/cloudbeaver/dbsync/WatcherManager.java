package com.cloudbeaver.dbsync;

import org.apache.log4j.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;

/**
 * Created by gaobin on 16-4-7.
 */
public class WatcherManager {

    private Logger logger = Logger.getLogger(WatcherManager.class);

    private Map<String, String> conf;
    private ArrayList<DbWatcher> databases;

    public Map<String, String> getConf() {
        return conf;
    }

    public void setConf(Map<String, String> conf) {
        this.conf = conf;
        ArrayList<DbWatcher> dbWatchersToRemove = new ArrayList<DbWatcher>();
        for (DbWatcher dbWatcher : databases) {
            if (conf.get("db." + dbWatcher.db + ".url").equalsIgnoreCase("null")) {
                dbWatchersToRemove.add(dbWatcher);
            }
        }
        databases.removeAll(dbWatchersToRemove);
        for (DbWatcher dbWatcher : databases) {
            dbWatcher.setDbExtractor(
                    conf.get("db." + dbWatcher.db + ".url"),
                    conf.get("db." + dbWatcher.db + ".username"),
                    conf.get("db." + dbWatcher.db + ".password")
            );
        }
    }

    public ArrayList<DbWatcher> getDatabases() {
        return databases;
    }

    public void setDatabases(ArrayList<DbWatcher> databases) {
        this.databases = databases;
    }

    public String query() {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (DbWatcher dbWatcher : databases) {
            System.out.println(dbWatcher.getDb());
            logger.debug("Query database " + dbWatcher.db + " .");
            String res = dbWatcher.query();
            if (res.length() > 2) {
                sb.append(res).append(',');
                //System.out.println(res);
            }
        }
        if (sb.charAt(sb.length()-1) == ',') {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append(']');
        return sb.toString();
    }
}
