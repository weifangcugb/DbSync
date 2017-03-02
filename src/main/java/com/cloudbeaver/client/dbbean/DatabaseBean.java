package com.cloudbeaver.client.dbbean;

import org.apache.log4j.*;
import org.javatuples.Triplet;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * bean for one database
 */
public class DatabaseBean implements Serializable{
    private static Logger logger = Logger.getLogger(DatabaseBean.class);

    List<TableBean> tables = new ArrayList<TableBean>();

    String rowversion;
    @JsonIgnore
    String prison;
    String db;

    @JsonIgnore
    String queryTime;

//  db configs
    @JsonIgnore
    String dbUrl;
    @JsonIgnore
    String dbUserName;
    @JsonIgnore
    String dbPassword;

    @JsonIgnore
    String appKey;
    @JsonIgnore
    String appSecret;
    
//  there are two types now, 'sqlServerDB' and 'oracleDB' and 'urlDB' and 'postgresDB'
    @JsonIgnore
    String type;

	//triplet<tableName, id, columnNmae> => value
    @JsonIgnore
	private ConcurrentHashMap<Triplet<String, String, String>, String> opTableCopy = new ConcurrentHashMap<>();

    @JsonIgnore
	public String getOpTableValue(String tableName, String rowKey, String columnName) {
		return opTableCopy.get(new Triplet<String, String, String>(tableName, rowKey, columnName));
	}

	public void putOpTableValue(String tableName, String rowKey, String columnName, String value) {
		Triplet<String, String, String> key = new Triplet<String, String, String>(tableName, rowKey, columnName);
		if (value != null && (!opTableCopy.contains(key) || !opTableCopy.get(key).equals(value))) {
			opTableCopy.put(key, value);
		}
	}

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
		assert(type != null);
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getDbUrl() {
		assert(dbUrl != null);
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

    public List<TableBean> getTables() {
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