package com.cloudbeaver.hdfsHttpProxy;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import com.cloudbeaver.client.common.BeaverUtils;

import net.sf.json.JSONObject;

public class HdfsProxyClient {
	private static Logger logger = Logger.getLogger(HdfsProxyClient.class);
	private static String fileInfoUrl = "http://localhost/fileinfo";

	public void doUploadFileData(String fileFullName, String urlString) {
		while(true) {
			try{
//				first, sync position with web server
		        long seekPos = 0;
				String fileName = fileFullName.substring(fileFullName.lastIndexOf("/")+1);
				String json = BeaverUtils.doGet(fileInfoUrl + "?fileName=" + fileName);
				JSONObject jsonObject = JSONObject.fromObject(json);
				if(fileName.equals(jsonObject.get("file")) && !jsonObject.get("length").equals(-1)){
					 seekPos = Long.valueOf(jsonObject.get("length").toString());
				}

//				then, open the url stream and write util the end of the file
				BeaverUtils.doPostBigFile(urlString, fileName, seekPos);
			}catch(IOException e){
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
