package com.cloudbeaver.hdfsHttpProxy.proxybean;

public class HdfsProxyClientInfoConf {
	private String fileInfoUrl;
	private String userName;
	private String passWd;
	private String tableUrl;

	public String getTableUrl() {
		return tableUrl;
	}
	public void setTableUrl(String tableUrl) {
		this.tableUrl = tableUrl;
	}
	public String getFileInfoUrl() {
		return fileInfoUrl;
	}
	public void setFileInfoUrl(String fileInfoUrl) {
		this.fileInfoUrl = fileInfoUrl;
	}
	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	public String getPassWd() {
		return passWd;
	}
	public void setPassWd(String passWd) {
		this.passWd = passWd;
	}
}
