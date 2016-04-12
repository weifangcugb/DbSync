package com.cloudbeaver.dbsync;

import org.apache.log4j.*;

import java.util.ArrayList;
import java.util.Map;

/**
 * Created by gaobin on 16-4-7.
 */
public class WatcherManager {

    private Logger logger = Logger.getLogger(WatcherManager.class);

    private Map<String, String> conf;
    private ArrayList<DbWatcher> databases;

    private Map<DbWatcher, DbExtractor> dbConnectionBank;

    public Map<String, String> getConf() {
        return conf;
    }

    public void setConf(Map<String, String> conf) {
        this.conf = conf;
        for (DbWatcher dbWatcher : databases) {
            dbWatcher.setDbExtractor(new DbExtractor(
                    conf.get("db." + dbWatcher.db + ".url"),
                    conf.get("db." + dbWatcher.db + ".username"),
                    conf.get("db." + dbWatcher.db + ".password")
            ));
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
