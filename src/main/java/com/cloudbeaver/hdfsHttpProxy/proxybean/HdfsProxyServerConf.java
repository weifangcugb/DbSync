package com.cloudbeaver.hdfsHttpProxy.proxybean;

public class HdfsProxyServerConf {
	private int httpPort;
	private int httpsPort;
	private int bufferSize;
	private String dbName;
	private String dbUser;
	private String dbPass;
	private String dbUrl;
	private String dbType;
	private String keyStorePath;
	private String keyStorePass;

	public int getHttpPort() {
		assert(httpPort > 0);
		return httpPort;
	}

	public void setHttpPort(int http_port) {
		this.httpPort = http_port;
	}

	public int getHttpsPort() {
		assert(httpsPort > 0);
		return httpsPort;
	}

	public void setHttpsPort(int https_port) {
		this.httpsPort = https_port;
	}

	public int getBufferSize() {
		assert(bufferSize > 1024);
		return bufferSize;
	}

	public void setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
	}

	public String getDbName() {
		assert(dbName != null);
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getDbUser() {
		assert(dbUser != null);
		return dbUser;
	}

	public void setDbUser(String dbUser) {
		this.dbUser = dbUser;
	}

	public String getDbPass() {
		assert(dbPass != null);
		return dbPass;
	}

	public void setDbPass(String dbPass) {
		this.dbPass = dbPass;
	}

	public String getDbUrl() {
		assert(dbUrl != null);
		return dbUrl;
	}

	public void setDbUrl(String dbUrl) {
		this.dbUrl = dbUrl;
	}

	public String getDbType() {
		assert(dbType != null);
		return dbType;
	}

	public void setDbType(String dbType) {
		this.dbType = dbType;
	}

	public String getKeyStorePath() {
		assert(keyStorePath != null);
		return keyStorePath;
	}

	public void setKeyStorePath(String keyStorePath) {
		this.keyStorePath = keyStorePath;
	}

	public String getKeyStorePass() {
		assert(keyStorePass != null);
		return keyStorePass;
	}

	public void setKeyStorePass(String keyStorePass) {
		this.keyStorePass = keyStorePass;
	}
}
