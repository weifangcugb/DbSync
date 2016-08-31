package com.cloudbeaver.hdfsHttpProxy;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;

import com.cloudbeaver.client.common.BeaverUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.org.apache.bcel.internal.generic.RETURN;

public class HdfsClient {
	private static Logger logger = Logger.getLogger(HdfsClient.class);
	private static String fileInfoUrl = "http://localhost:8811/fileinfo";
	private static int READ_BUFFER_SIZE = 1024 * 1024;
//	private static int READ_BUFFER_SIZE = 100;
	private static String contentType = "application/octet-stream";
	private static int UPLOAD_RETRY_TIMES = 16;
	private long UPLOAD_ERROR_SLEEP_TIME = 5000;
	protected boolean EXCEPTION_TEST_MODE = false;

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

	public void uploadFileDataWithLength(String fileFullName, String url, int size){
		int totalSize = 0;
		String fileName = fileFullName.substring(fileFullName.lastIndexOf("/")+1);
		RandomAccessFile in;
		boolean doesUploadSueeccd = true;
		try {
			String json = BeaverUtils.doGet(fileInfoUrl+"?fileName="+fileName);
			Map<String, FileInfoBean> fileInfoMap = jsonToMap(json);
			in = new RandomAccessFile(fileFullName,"r");
			if(!fileInfoMap.isEmpty() && fileInfoMap.containsKey(fileName)){
				System.out.println("offset : " + fileInfoMap.get(fileName).offset);
				in.seek(fileInfoMap.get(fileName).offset);
			}
	        byte [] readBuf = new byte[READ_BUFFER_SIZE];
	        int readCount = 0;
	        int len = 0;
	        while(readCount < READ_BUFFER_SIZE && (len = in.read(readBuf, readCount, READ_BUFFER_SIZE - readCount)) != -1){
	        	readCount += len;
	 			if(readCount == READ_BUFFER_SIZE){
	 				doesUploadSueeccd = retryUploadDataToServer(fileName,url,readBuf,0,readCount);
	 				if(!doesUploadSueeccd){
	 					in.close();
	 					return;
	 				}
	 				totalSize += readCount;
	 				readCount = 0;
	 				if(totalSize >= size){
	 					break;
	 				}
	 			}
	    	}
	        if(totalSize < size && readCount > 0){
	        	doesUploadSueeccd = retryUploadDataToServer(fileName,url,readBuf,0,readCount);
	        	if(!doesUploadSueeccd){
 					in.close();
 					return;
 				}
	        	totalSize += readCount;
	        }
	        in.close();
		} catch (IOException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("could not open file or seek to offset!");
		}        
	}

	public boolean retryUploadDataToServer(String fileName, String url, byte[] readBuf, int startPos, int readCount) {
		for (int i = 1; i <= UPLOAD_RETRY_TIMES ; i++) {
			try {
				logger.info("start to upload file to server");
//				System.out.println(new String(readBuf, 0, readCount, "UTF-8"));
				BeaverUtils.doPost(url, contentType, false, readBuf, startPos, readCount);
				logger.info("finish upload file to server");
				break;
			} catch (IOException e) {
				BeaverUtils.PrintStackTrace(e);
				logger.error("upload file data error. msg:" + e.getMessage());
				if(i >= UPLOAD_RETRY_TIMES){
					return false; 
				}
				BeaverUtils.sleep(UPLOAD_ERROR_SLEEP_TIME );
			}	
		}
		return true;
	}

	public void doUploadFileData(String fileFullName, String url) {
		while(true) {
			try{
//				first, sync position with web server
				String fileName = fileFullName.substring(fileFullName.lastIndexOf("/")+1);
				String json = BeaverUtils.doGet(fileInfoUrl+"?fileName="+fileName);
				Map<String, FileInfoBean> fileInfoMap = jsonToMap(json);				
				long seekPos = 0;

//				then, open the url stream and write util the end of the file
				RandomAccessFile in = new RandomAccessFile(fileFullName,"r"); 
				if(!fileInfoMap.isEmpty() && fileInfoMap.containsKey(fileName)){
					seekPos = fileInfoMap.get(fileName).offset;
					in.seek(seekPos);
				}

				logger.info("start to upload file, fileName:" + fileName + " pos:" + seekPos);
		        byte [] readBuf = new byte[READ_BUFFER_SIZE];
		        int readCount = 0, len = 0;
		        while(readCount < READ_BUFFER_SIZE && (len = in.read(readBuf, readCount, READ_BUFFER_SIZE - readCount)) != -1){
		        	readCount += len;
		 			if(readCount == READ_BUFFER_SIZE){
		 				BeaverUtils.doPost(url, contentType, false, readBuf, 0, readCount);
		 				readCount = 0;
		 			}
		    	}

		        if(readCount > 0){
		        	BeaverUtils.doPost(url, contentType, false, readBuf, 0, readCount);
		        }
		
		        in.close();
		        break;
			}catch(IOException e){
				BeaverUtils.printLogExceptionAndSleep(e, "upload file exception, ", 5000);
			}
		}
	}

	public static void main(String[] args) {
		String filename = "/home/beaver/Documents/test/hadoop/harry.txt";
		String url = "http://localhost:8811/uploaddata?fileName=" + filename.substring(filename.lastIndexOf("/")+1);
		HdfsClient hdfsHttpClient = new HdfsClient();
		hdfsHttpClient.doUploadFileData(filename, url);
	}
}
