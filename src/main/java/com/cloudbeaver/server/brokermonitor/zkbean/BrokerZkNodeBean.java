package com.cloudbeaver.server.brokermonitor.zkbean;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.*;

public class BrokerZkNodeBean {
	private String ids;
	@JsonIgnore
	private int jmx_port;
	@JsonIgnore
	private String timestamp;
	@JsonIgnore
	private ArrayList<String> endpoints;
	private String host;
	@JsonIgnore
	private int version;
	private int port;
	
	public BrokerZkNodeBean () {}
	
	public String getIds () {
		return ids;
	}
	public void setIds (String x) {
		ids = x;
	}
	public int getJmx_port () {
		return jmx_port;
	}
	public String getTimestamp () {
		return timestamp;
	}
	public ArrayList<String> getEndpoints() {
		return endpoints;
	}
	public String getHost () {
		return host;
	}
	public int getVersion () {
		return version;
	}
	public int getPort () {
		return port;
	}
	/*public String toString () {
		return "lsx [ host: " + host + ", port: " + port + " ]";
	}*/
}
