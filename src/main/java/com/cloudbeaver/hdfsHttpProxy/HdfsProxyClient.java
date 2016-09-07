package com.cloudbeaver.hdfsHttpProxy;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.junit.Assert;
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
	private String tableUrl;

	public void doUploadFileData(String fileFullName, String urlString) {
		ApplicationContext appContext = new FileSystemXmlApplicationContext("conf/HdfsProxyClientInfoConf.xml");
		HdfsProxyClientInfoConf hdfsProxyInfoConf = appContext.getBean("HdfsProxyClientInfoConf", HdfsProxyClientInfoConf.class);
		fileInfoUrl = hdfsProxyInfoConf.getFileInfoUrl();
		userName = hdfsProxyInfoConf.getUserName();
		passWd = hdfsProxyInfoConf.getPassWd();
		tableUrl = hdfsProxyInfoConf.getTableUrl();
		Assert.assertNotNull("FileInfoUrl is null", hdfsProxyInfoConf.getFileInfoUrl());
		Assert.assertNotNull("Password is null", hdfsProxyInfoConf.getPassWd());
		Assert.assertNotNull("User name is null", hdfsProxyInfoConf.getUserName());
		Assert.assertNotNull("TableUrl is null", hdfsProxyInfoConf.getTableUrl());
		Assert.assertNotEquals("FileInfoUrl is empty", "", hdfsProxyInfoConf.getFileInfoUrl());
		Assert.assertNotEquals("User name is empty", "", hdfsProxyInfoConf.getUserName());
		Assert.assertNotEquals("Password is empty", "", hdfsProxyInfoConf.getPassWd());
		Assert.assertNotEquals("TableUrl is empty", "", hdfsProxyInfoConf.getTableUrl());

		while(true) {
			try{
				JSONObject jsonObject = new JSONObject();
		    	jsonObject.put("userName", userName);
		    	jsonObject.put("passWd", passWd);
		    	jsonObject.put("tableUrl", tableUrl);

//				first, sync position with web server
		        long seekPos = 0;
		        byte[] token = null;
				String fileName = fileFullName.substring(fileFullName.lastIndexOf("/")+1);
				jsonObject.put("fileName", fileName);
				String json = BeaverUtils.doPost(fileInfoUrl, jsonObject.toString(), true);
				jsonObject = JSONObject.fromObject(json);
				if(jsonObject.containsKey("fileName") && jsonObject.containsKey("length") && jsonObject.containsKey("errorCode") && jsonObject.containsKey("token")){
					if(jsonObject.get("errorCode").equals(0)){
						token = org.apache.commons.codec.binary.Base64.decodeBase64(jsonObject.getString("token"));
						logger.info("token = " + token);
						if(fileName.equals(jsonObject.get("fileName")) && jsonObject.getLong("length") > -1){
							 seekPos = Long.valueOf(jsonObject.get("length").toString());
						}
						else if(!fileName.equals(jsonObject.get("fileName"))){
							throw new IllegalArgumentException("file doesn't match");
						}
					}
					else if(jsonObject.get("errorCode").equals(5)){
						throw new SQLException("connect to database failed");
					}
				}
				else{
					throw new IllegalArgumentException("missing argument filename or length or errorCode or token");
				}

//				then, open the url stream and write util the end of the file
				if(token != null){
					BeaverUtils.doPostBigFile(urlString, fileFullName, seekPos, token, token.length);
				}
				else{
					throw new IllegalArgumentException("token is null");
				}
				break;
			}catch(IOException | SQLException e){
				logger.error("upload file failed");
				BeaverUtils.printLogExceptionAndSleep(e, "upload file failed", 5000);
			}
		}
	}

	public static void main(String[] args) {
		String filename = "/home/beaver/Documents/test/hadoop/harry.txt";
		String url = "https://localhost:8811/uploaddata?fileName=" + filename.substring(filename.lastIndexOf("/") + 1);
		HdfsProxyClient hdfsHttpClient = new HdfsProxyClient();
		hdfsHttpClient.doUploadFileData(filename, url);		
	}
}
