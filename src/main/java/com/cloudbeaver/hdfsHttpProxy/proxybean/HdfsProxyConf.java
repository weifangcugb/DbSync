package com.cloudbeaver.hdfsHttpProxy.proxybean;

public class HdfsProxyConf {
	private int port;
	private static int bufferSize;

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public static int getBufferSize() {
		return bufferSize;
	}

	public static void setBufferSize(int bufferSize) {
		HdfsProxyConf.bufferSize = bufferSize;
	}
}
