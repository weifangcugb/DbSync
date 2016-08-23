package com.cloudbeaver.hdfsHttpProxy;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;

import com.cloudbeaver.client.common.BeaverUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class HdfsHttpProxy {
	private static Logger logger = Logger.getLogger(HdfsHttpProxy.class);
	private static String fileInfoUrl = "http://localhost:8090/fileinfo";
	private static int READ_BUFFER_SIZE = 1024 * 1024;
//	private static int READ_BUFFER_SIZE = 100;
	private static String contentType = "application/octet-stream";

	public static String startUploadFileData(String urlString, byte[] data, int len) throws IOException{
		BufferedReader br = null;
		HttpURLConnection urlConnection = null;
		try {
			if (urlString.indexOf("http://") == -1) {
				urlString = "http://" + urlString;
			}

	        URL url = new URL(urlString);
	        urlConnection = (HttpURLConnection) url.openConnection();
	        urlConnection.setRequestMethod("POST");
	        urlConnection.setRequestProperty("Content-Type", contentType + ";charset=utf-8");
	        urlConnection.setRequestProperty("Content-Length", "" + data.length);
	        urlConnection.setConnectTimeout(20000);
	        urlConnection.setDoInput(true);
	        logger.debug(urlConnection.getRequestProperty("Content-Type"));

	        if (data != null) {
		        urlConnection.setDoOutput(true);
		        DataOutputStream out=new DataOutputStream((urlConnection.getOutputStream()));
		        out.write(data, 0, len);
		        out.flush();
		        out.close();
			}
	        BufferedReader in = new BufferedReader(new InputStreamReader(urlConnection.getInputStream()));
	        String line = "";
	        StringBuilder sb = new StringBuilder();
	        while ((line = in.readLine()) != null) {
	            sb.append(line);
	        }
	        logger.debug("Got reply message from web, server:" + urlString + " responseCode:" + urlConnection.getResponseCode() + " reply:" + sb.toString());
	        return sb.toString();
		}finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					logger.error("close url reader error, msg:" + e.getMessage() + " url:" + urlString);
				}
			}
		}
	}

	public Map<String, FileInfoBean> jsonToMap(String json) {
		ObjectMapper mapper = new ObjectMapper();
		HashMap<String, FileInfoBean> fileInfo = new HashMap<String, FileInfoBean>();
		try {
			fileInfo = mapper.readValue(json, new TypeReference<Map<String, FileInfoBean>>(){});
		} catch (IOException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("change json to map failed!");
		}		
		return fileInfo;
	}

	public void uploadFileData(String filename, String url) throws IOException{
		String json = BeaverUtils.doGet(fileInfoUrl);
		Map<String, FileInfoBean> fileInfoMap = jsonToMap(json);
		try{
			RandomAccessFile in = new RandomAccessFile(filename,"r"); 
			if(!fileInfoMap.isEmpty() && fileInfoMap.containsKey(filename)){
				in.seek(fileInfoMap.get(filename).offset);
			}
            byte [] readBuf = new byte[READ_BUFFER_SIZE];
            int readCount = 0;
            int len = 0;
            while(readCount < READ_BUFFER_SIZE && (len = in.read(readBuf, readCount, READ_BUFFER_SIZE - readCount)) != -1){
            	readCount += len;
	 			if(readCount == READ_BUFFER_SIZE){
	 				startUploadFileData(url, readBuf, READ_BUFFER_SIZE);
	 				readCount = 0;
	 			}
	    	}
            if(readCount > 0){
            	startUploadFileData(url, readBuf, readCount);
            }
            in.close();
        } catch (Exception e){
        	BeaverUtils.PrintStackTrace(e);
			logger.error("read data from local failed!");
        }
	}

	public void uploadFileData(String filename, String url, int size) throws IOException{
		int totalSize = 0;
		String name = filename.substring(filename.lastIndexOf("/")+1);
		String json = BeaverUtils.doGet(fileInfoUrl);
		Map<String, FileInfoBean> fileInfoMap = jsonToMap(json);
		try{
			RandomAccessFile in = new RandomAccessFile(filename,"r"); 
			if(!fileInfoMap.isEmpty() && fileInfoMap.containsKey(name)){
				in.seek(fileInfoMap.get(name).offset);
			}
            byte [] readBuf = new byte[READ_BUFFER_SIZE];
            int readCount = 0;
            int len = 0;
            while(readCount < READ_BUFFER_SIZE && (len = in.read(readBuf, readCount, READ_BUFFER_SIZE - readCount)) != -1){
            	readCount += len;
	 			if(readCount == READ_BUFFER_SIZE){
	 				startUploadFileData(url, readBuf, readCount);
	 				totalSize += readCount;
	 				readCount = 0;
	 				if(totalSize >= size){
	 					break;
	 				}
	 			}
	    	}
            if(totalSize < size && readCount > 0){
            	startUploadFileData(url, readBuf, readCount);
            	totalSize += readCount;
            }
            in.close();
        } catch (Exception e){
        	BeaverUtils.PrintStackTrace(e);
			logger.error("read data from local failed!");
        }
	}

	public static void main(String[] args) {
		String filename = "/home/beaver/Documents/test/hadoop/test.txt";
		String url = "http://localhost:8090/uploaddata?filename=" + filename.substring(filename.lastIndexOf("/")+1);
		HdfsHttpProxy hdfsHttpProxy = new HdfsHttpProxy();
		try {
			hdfsHttpProxy.uploadFileData(filename.substring(filename.lastIndexOf("/")+1), url);
		} catch (IOException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("upload data to server failed!");
		}
	}
}
