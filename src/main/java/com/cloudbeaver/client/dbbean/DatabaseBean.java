package com.cloudbeaver.client.dbbean;

import org.apache.log4j.*;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * bean for one database
 */
public class DatabaseBean implements Serializable{
    private static Logger logger = Logger.getLogger(DatabaseBean.class);

    ArrayList<TableBean> tables = new ArrayList<TableBean>();

    String rowversion;
    String prison;
    String db;

    String queryTime;

//  db configs
    String dbUrl;
    String dbUserName;
    String dbPassword;

    String appKey;
    String appSecret;
    
//  there are two types now, 'sqlServerDB' and 'oracleDB' and 'urlDB'
    String type;

    public String getAppKey() {
		return appKey;
	}

	public void setAppKey(String appKey) {
		this.appKey = appKey;
	}

	public String getAppSecret() {
		return appSecret;
	}

	public void setAppSecret(String appSecret) {
		this.appSecret = appSecret;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

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

    public void setQueryTime(String touchTime){
    	this.queryTime = touchTime;
    }

	public String getQueryTime() {
		return queryTime;
	}
}
