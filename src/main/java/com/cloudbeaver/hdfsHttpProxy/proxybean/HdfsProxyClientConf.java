package com.cloudbeaver.hdfsHttpProxy.proxybean;

public class HdfsProxyClientConf {
	private String fileInfoUrl;
	private String userName;
	private String passWd;
	private String tableUrl;
	private String fileLocalPath;
	private String uploadFileUrl;

	public String getUploadFileUrl() {
		return uploadFileUrl;
	}
	public void setUploadFileUrl(String uploadFileUrl) {
		this.uploadFileUrl = uploadFileUrl;
	}
	public String getFileLocalPath() {
		assert(fileLocalPath != null);
		return fileLocalPath;
	}
	public void setFileLocalPath(String fileLocalPath) {
		this.fileLocalPath = fileLocalPath;
	}
	public String getTableUrl() {
		assert(tableUrl != null && tableUrl.startsWith("upload://table/"));
		return tableUrl;
	}
	public void setTableUrl(String tableUrl) {
		this.tableUrl = tableUrl;
	}
	public String getFileInfoUrl() {
		assert(fileInfoUrl != null);
		return fileInfoUrl;
	}
	public void setFileInfoUrl(String fileInfoUrl) {
		this.fileInfoUrl = fileInfoUrl;
	}
	public String getUserName() {
		assert(userName != null);
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	public String getPassWd() {
		assert(passWd != null);
		return passWd;
	}
	public void setPassWd(String passWd) {
		this.passWd = passWd;
	}
}
