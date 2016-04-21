package com.cloudbeaver.client.dbbean;

import org.apache.log4j.*;

import java.util.ArrayList;

/**
 * bean for one database
 */
public class DatabaseBean {
    private static Logger logger = Logger.getLogger(DatabaseBean.class);

    ArrayList<TableBean> tables = new ArrayList<TableBean>();
    String rowversion = null;
    String prison = null;
    String db = null;

//    db configs
    String dbUrl = null;
    String dbUserName = null;
    String dbPassword = null;

    public String getDbUrl() {
		return dbUrl;
	}

	public void setDbUrl(String dbUrl) {
		this.dbUrl = dbUrl;
	}

	public String getDbUserName() {
		return dbUserName;
	}

	public void setDbUserName(String dbUserName) {
		this.dbUserName = dbUserName;
	}

	public String getDbPassword() {
		return dbPassword;
	}

	public void setDbPassword(String dbPassword) {
		this.dbPassword = dbPassword;
	}

	public String toString(){
    	return db + "," + prison + "," + rowversion;
    }

    public ArrayList<TableBean> getTables() {
        return tables;
    }

    public void setTables(ArrayList<TableBean> tables) {
        this.tables = tables;
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
}
