package com.cloudbeaver.client.common;

public class SyncClientConfBean {
	private String clientId;
	private String taskServerUrl;
	private String flumeServerUrl;

	public String getClientId() {
		assert(clientId != null);
		return clientId;
	}
	public void setClientId(String clientId) {
		this.clientId = clientId;
	}
	public String getTaskServerUrl() {
		assert(taskServerUrl != null);
		return taskServerUrl;
	}
	public void setTaskServerUrl(String taskServerUrl) {
		this.taskServerUrl = taskServerUrl;
	}
	public String getFlumeServerUrl() {
		assert(flumeServerUrl != null);
		return flumeServerUrl;
	}
	public void setFlumeServerUrl(String flumeServerUrl) {
		this.flumeServerUrl = flumeServerUrl;
	}	
}
