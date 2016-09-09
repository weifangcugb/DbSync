package com.cloudbeaver.hdfsHttpProxy;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.client.common.CommonUploader;
import com.cloudbeaver.client.common.BeaverUtils.ErrCode;
import com.cloudbeaver.hdfsHttpProxy.proxybean.HdfsProxyClientConf;
import net.sf.json.JSONObject;

public class HdfsProxyClient {
	private static Logger logger = Logger.getLogger(HdfsProxyClient.class);

	private static final String CONF_BEAN = "HdfsProxyClientConf";
	private static final String CONF_FILE = CommonUploader.CONF_FILE_DIR + CONF_BEAN + ".xml";

	public void doUploadFileData(String fileFullName, String urlString) {
		ApplicationContext appContext = new FileSystemXmlApplicationContext(CONF_FILE);
		HdfsProxyClientConf hdfsProxyInfoConf = appContext.getBean(CONF_BEAN, HdfsProxyClientConf.class);

		while(true) {
			try{
				JSONObject jsonObject = new JSONObject();
		    	jsonObject.put("userName", hdfsProxyInfoConf.getUserName());
		    	jsonObject.put("passWd", hdfsProxyInfoConf.getPassWd());
		    	jsonObject.put("tableId", BeaverUtils.getTableIdFromUploadUrl(hdfsProxyInfoConf.getTableUrl()));
		    	String fileName = fileFullName.substring(fileFullName.lastIndexOf("/")+1);
				jsonObject.put("fileName", fileName);

//				first, sync position with web server
				String json = BeaverUtils.doPost(hdfsProxyInfoConf.getFileInfoUrl(), jsonObject.toString(), true);
				jsonObject = JSONObject.fromObject(json);
				if(jsonObject.containsKey("errorCode") && jsonObject.getInt("errorCode") == 0 && jsonObject.containsKey("fileName") 
						&& jsonObject.containsKey("offset") && jsonObject.containsKey("token")
						&& fileName.equals(jsonObject.get("fileName")) && jsonObject.getLong("offset") > -1){
					BeaverUtils.doPostBigFile(urlString + "&token=" + jsonObject.getString("token"), fileFullName, jsonObject.getLong("offset"));
					break;
				} else {
					if(jsonObject.getInt("errorCode") == ErrCode.SQL_ERROR.ordinal()){
						throw new SQLException("connect to database failed");
					}else{
						throw new IOException("missing argument filename or length or errorCode or token or server response error");
					}
				}
			} catch(IOException | SQLException e) {
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
