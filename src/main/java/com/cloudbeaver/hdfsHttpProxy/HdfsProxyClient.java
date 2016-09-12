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

	public void doUploadFileData() {
		ApplicationContext appContext = new FileSystemXmlApplicationContext(CONF_FILE);
		HdfsProxyClientConf hdfsProxyInfoConf = appContext.getBean(CONF_BEAN, HdfsProxyClientConf.class);

		String localFileName = hdfsProxyInfoConf.getFileLocalPath();
		String uploadFileUrl = hdfsProxyInfoConf.getUploadFileUrl();

		while(true) {
			try{
//				first, sync position with web server
				JSONObject jsonObject = new JSONObject();
		    	jsonObject.put(HdfsProxyServer.EMAIL, hdfsProxyInfoConf.getUserName());
		    	jsonObject.put(HdfsProxyServer.PASSWORD, hdfsProxyInfoConf.getPassWd());
		    	jsonObject.put(HdfsProxyServer.TABLEID, BeaverUtils.getTableIdFromUploadUrl(hdfsProxyInfoConf.getTableUrl()));
				String json = BeaverUtils.doPost(hdfsProxyInfoConf.getFileInfoUrl(), jsonObject.toString(), true);
				JSONObject responseObject = JSONObject.fromObject(json);

//				second, upload file
				if(responseObject.containsKey(HdfsProxyServer.ERRORCODE) && responseObject.getInt(HdfsProxyServer.ERRORCODE) == 0 && responseObject.containsKey(HdfsProxyServer.OFFSET) 
						&& responseObject.containsKey(HdfsProxyServer.TOKEN) && responseObject.getLong(HdfsProxyServer.OFFSET) >= 0){
					BeaverUtils.doPostBigFile(uploadFileUrl + "?" +HdfsProxyServer.TOKEN + "=" + responseObject.getString(HdfsProxyServer.TOKEN), localFileName, responseObject.getLong(HdfsProxyServer.OFFSET));
					break;
				}else{
					throw new IOException(BeaverUtils.ErrCode.getErrCode(responseObject.getInt(HdfsProxyServer.ERRORCODE)).getErrMsg());
				}
			} catch(IOException e) {
				logger.error("upload file failed");
				BeaverUtils.printLogExceptionAndSleep(e, "upload file failed", 5000);
			}
		}
	}

	public static void main(String[] args) {
		HdfsProxyClient hdfsHttpClient = new HdfsProxyClient();
		hdfsHttpClient.doUploadFileData();		
	}
}
