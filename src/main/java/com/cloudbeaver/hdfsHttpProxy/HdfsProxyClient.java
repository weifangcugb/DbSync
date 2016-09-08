package com.cloudbeaver.hdfsHttpProxy;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.hdfsHttpProxy.proxybean.HdfsProxyClientInfoConf;
import net.sf.json.JSONObject;

public class HdfsProxyClient {
	private static Logger logger = Logger.getLogger(HdfsProxyClient.class);

	public void doUploadFileData(String fileFullName, String urlString) {
		ApplicationContext appContext = new FileSystemXmlApplicationContext("conf/HdfsProxyClientInfoConf.xml");
		HdfsProxyClientInfoConf hdfsProxyInfoConf = appContext.getBean("HdfsProxyClientInfoConf", HdfsProxyClientInfoConf.class);

		while(true) {
			try{
				JSONObject jsonObject = new JSONObject();
		    	jsonObject.put("userName", hdfsProxyInfoConf.getUserName());
		    	jsonObject.put("passWd", hdfsProxyInfoConf.getPassWd());
		    	jsonObject.put("tableUrl", hdfsProxyInfoConf.getTableUrl());
		    	String fileName = fileFullName.substring(fileFullName.lastIndexOf("/")+1);
				jsonObject.put("fileName", fileName);

//				first, sync position with web server
		        long seekPos = 0;
		        byte[] token = null;
				String json = BeaverUtils.doPost(hdfsProxyInfoConf.getFileInfoUrl(), jsonObject.toString(), true);
				jsonObject = JSONObject.fromObject(json);
				if(jsonObject.containsKey("fileName") && jsonObject.containsKey("length") && jsonObject.containsKey("errorCode") && jsonObject.containsKey("token")){
					if(jsonObject.get("errorCode").equals(0)){
						token = Base64.decodeBase64(jsonObject.getString("token"));
						logger.info("token = " + token);
						if(fileName.equals(jsonObject.get("fileName")) && jsonObject.getLong("length") > -1){
							 seekPos = jsonObject.getLong("length");
						} else if(!fileName.equals(jsonObject.get("fileName"))){
							throw new IllegalArgumentException("file doesn't match");
						}
					} else if(jsonObject.get("errorCode").equals(5)){
						throw new SQLException("connect to database failed");
					}
				} else {
					throw new IllegalArgumentException("missing argument filename or length or errorCode or token");
				}

//				then, open the url stream and write util the end of the file
				if(token != null){
					BeaverUtils.doPostBigFile(urlString, fileFullName, seekPos, token, token.length);
				} else{
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
