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
	private static int UPLOAD_RETRY_TIMES = 16;
	private long UPLOAD_ERROR_SLEEP_TIME = 5000;
	protected boolean EXCEPTION_TEST_MODE = false;

	public String startUploadFileData(String urlString, byte[] data, int startPos, int len) throws IOException{
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
	        urlConnection.setDoOutput(true);
	        logger.debug(urlConnection.getRequestProperty("Content-Type"));

	        if (data != null) {
		        DataOutputStream out = new DataOutputStream((urlConnection.getOutputStream()));
		        out.write(data, startPos, len/2);
		        out.flush();
		        logger.info("write half of the data");
		        if(EXCEPTION_TEST_MODE){
		        	throw new IOException();
		        }
		        out.write(data, startPos + len/2, len - len/2);
		        logger.info("write left half of the data");
		        out.flush();
		        out.close();
			}

	        String line = "";
	        StringBuilder sb = new StringBuilder();
	        BufferedReader in = new BufferedReader(new InputStreamReader(urlConnection.getInputStream()));
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
		String name = filename.substring(filename.lastIndexOf("/")+1);
		try{
			RandomAccessFile in = new RandomAccessFile(filename,"r"); 
			if(!fileInfoMap.isEmpty() && fileInfoMap.containsKey(filename)){
				System.out.println(fileInfoMap.get(filename).offset);
				in.seek(fileInfoMap.get(filename).offset);
			}
            byte [] readBuf = new byte[READ_BUFFER_SIZE];
            int readCount = 0;
            int len = 0;
            while(readCount < READ_BUFFER_SIZE && (len = in.read(readBuf, readCount, READ_BUFFER_SIZE - readCount)) != -1){
            	readCount += len;
	 			if(readCount == READ_BUFFER_SIZE){
	 				retryUploadDataToServer(name,url,readBuf,0,readCount);
	 				readCount = 0;
	 			}
	    	}
            if(readCount > 0){
            	retryUploadDataToServer(name,url,readBuf,0,readCount);
            }
            in.close();
        } catch (IOException e){
        	BeaverUtils.PrintStackTrace(e);
			logger.error("read data from local failed!");
        }
	}

	public void uploadFileDataWithLength(String filename, String url, int size){
		int totalSize = 0;
		String name = filename.substring(filename.lastIndexOf("/")+1);
		RandomAccessFile in;
		try {
			String json = BeaverUtils.doGet(fileInfoUrl);
			Map<String, FileInfoBean> fileInfoMap = jsonToMap(json);
			in = new RandomAccessFile(filename,"r");
			if(!fileInfoMap.isEmpty() && fileInfoMap.containsKey(name)){
				System.out.println("offset : " + fileInfoMap.get(name).offset);
				in.seek(fileInfoMap.get(name).offset);
			}
	        byte [] readBuf = new byte[READ_BUFFER_SIZE];
	        int readCount = 0;
	        int len = 0;
	        while(readCount < READ_BUFFER_SIZE && (len = in.read(readBuf, readCount, READ_BUFFER_SIZE - readCount)) != -1){
	        	readCount += len;
	 			if(readCount == READ_BUFFER_SIZE){
	 				retryUploadDataToServer(name,url,readBuf,0,readCount);
	 				totalSize += readCount;
	 				readCount = 0;
	 				if(totalSize >= size){
	 					break;
	 				}
	 			}
	    	}
	        if(totalSize < size && readCount > 0){
	        	retryUploadDataToServer(name,url,readBuf,0,readCount);
	        	totalSize += readCount;
	        }
	        in.close();
		} catch (IOException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("could not open file or seek to offset!");
		}        
	}

	public void retryUploadDataToServer(String filename, String url, byte[] readBuf, int startPos, int readCount) {
		int totalCount = readCount;
		for (int i = 0; i < UPLOAD_RETRY_TIMES ; i++) {
			try {
				logger.info("start to upload file to server");
//				System.out.println(new String(readBuf, 0, readCount, "UTF-8"));
				startUploadFileData(url, readBuf, startPos, readCount);
				logger.info("finish upload file to server");
				String json = BeaverUtils.doGet(fileInfoUrl);
				Map<String, FileInfoBean> fileInfoMap = jsonToMap(json);
				if(fileInfoMap.get(filename).bufferSize <= 0){
					break;
				}
				else{
					if(fileInfoMap.get(filename).offset % READ_BUFFER_SIZE == 0) {
						startPos = READ_BUFFER_SIZE;
					}
					else {
						startPos = fileInfoMap.get(filename).offset % READ_BUFFER_SIZE;
					}					
					readCount = totalCount - startPos;
				}
			} catch (IOException e) {
				BeaverUtils.PrintStackTrace(e);
				logger.error("upload file data error. msg:" + e.getMessage());
				BeaverUtils.sleep(UPLOAD_ERROR_SLEEP_TIME );
			}	
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
