package com.cloudbeaver.hdfsHttpProxy;

import java.io.IOException;
import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.hdfsHttpProxy.proxybean.HdfsProxyClientInfoConf;

import net.sf.json.JSONObject;

public class HdfsProxyClient {
	private static Logger logger = Logger.getLogger(HdfsProxyClient.class);
	private String fileInfoUrl;
	private String userName;
	private String passWd;

	public void doUploadFileData(String fileFullName, String urlString) {
		ApplicationContext appContext = new FileSystemXmlApplicationContext("conf/HdfsProxyClientInfoConf.xml");
		HdfsProxyClientInfoConf hdfsProxyInfoConf = appContext.getBean("HdfsProxyClientInfoConf", HdfsProxyClientInfoConf.class);
		fileInfoUrl = hdfsProxyInfoConf.getFileInfoUrl();
		userName = hdfsProxyInfoConf.getUserName();
		passWd = hdfsProxyInfoConf.getPassWd();
		while(true) {
			try{
				JSONObject jsonObject = new JSONObject();
		    	jsonObject.put("userName", userName);
		    	jsonObject.put("passWd", passWd);

//				first, sync position with web server
		        long seekPos = 0;
				String fileName = fileFullName.substring(fileFullName.lastIndexOf("/")+1);
				jsonObject.put("fileName", fileName);
				String json = BeaverUtils.doPost(fileInfoUrl, jsonObject.toString());
				jsonObject = JSONObject.fromObject(json);
				if(fileName.equals(jsonObject.get("fileName")) && !jsonObject.get("length").equals(-1)){
					 seekPos = Long.valueOf(jsonObject.get("length").toString());
				}

//				then, open the url stream and write util the end of the file
				BeaverUtils.doPostBigFile(urlString, fileFullName, seekPos);
				break;
			}catch(IOException e){
				logger.error("upload file failed");
				BeaverUtils.printLogExceptionAndSleep(e, "upload file failed", 5000);
			}
		}
	}

	public static void main(String[] args) {
		String filename = "/home/beaver/Documents/test/hadoop/harry.txt";
		String url = "http://localhost:8811/uploaddata?fileName=" + filename.substring(filename.lastIndexOf("/") + 1);
		HdfsProxyClient hdfsHttpClient = new HdfsProxyClient();
		hdfsHttpClient.doUploadFileData(filename, url);
	}
}
