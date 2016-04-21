package com.cloudbeaver.client.dbbean;

import org.apache.log4j.*;

import java.util.ArrayList;
import java.util.Map;

/*
 * root entry for beans, contains many db-beans
 */
public class MultiDatabaseBean {
    private Logger logger = Logger.getLogger(MultiDatabaseBean.class);

    private Map<String, String> conf;
    private ArrayList<DatabaseBean> databases;

    public Map<String, String> getConf() {
        return conf;
    }

    public void setConf(Map<String, String> conf) {
        this.conf = conf;

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
}