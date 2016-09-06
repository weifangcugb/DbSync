package com.cloudbeaver.hdfsHttpProxy.proxybean;

public class HdfsProxyServerConf {
	private int http_port;
	private int https_port;
	private static int bufferSize;

	public int getHttp_port() {
		return http_port;
	}

	public void setHttp_port(int http_port) {
		this.http_port = http_port;
	}

	public int getHttps_port() {
		return https_port;
	}

	public void setHttps_port(int https_port) {
		this.https_port = https_port;
	}

	public static int getBufferSize() {
		return bufferSize;
	}

	public static void setBufferSize(int bufferSize) {
		HdfsProxyServerConf.bufferSize = bufferSize;
	}
}
