package com.cloudbeaver.client.dbbean;

import org.apache.log4j.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/*
 * root entry for beans, contains many db-beans
 */
public class MultiDatabaseBean {
    private Logger logger = Logger.getLogger(MultiDatabaseBean.class);

    private List<DatabaseBean> databases;

    public List<DatabaseBean> getDatabases() {
        return databases;
    }

    public void setDatabases(ArrayList<DatabaseBean> databases) {
        this.databases = databases;
    }
}