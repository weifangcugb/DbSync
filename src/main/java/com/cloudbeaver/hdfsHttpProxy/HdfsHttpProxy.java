package com.cloudbeaver.hdfsHttpProxy;

import java.io.IOException;
import org.apache.log4j.Logger;
import com.cloudbeaver.client.common.BeaverUtils;

public class HdfsHttpProxy {
	private static Logger logger = Logger.getLogger(HdfsHttpProxy.class);
	private static String URL = "http://localhost:8090/filedata";
	public static String fileData = "Jetty has a slogan, Don't deploy your application in Jetty, deploy Jetty in your application.\n"
			+ " What this means is that as an alternative to bundling your application as a standard WAR to be deployed in Jetty, \n"
			+ "Jetty is designed to be a software component that can be instantiated and used in a Java program just like any POJO. \n"
			+ "Put another way, running Jetty in embedded mode means putting an HTTP module into your application, rather than putting "
			+ "your application into an HTTP server.";

	public static void startUploadFileData(String url, String data) throws IOException{
		BeaverUtils.doPost(url, data);
	}

	public static void main(String[] args) throws IOException{
		startUploadFileData(URL, fileData);
	}
}
