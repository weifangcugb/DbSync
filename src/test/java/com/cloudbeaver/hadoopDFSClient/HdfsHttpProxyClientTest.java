package com.cloudbeaver.hadoopDFSClient;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.BeaverCommonUtil;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.hdfsHttpProxy.HdfsProxyClient;

import net.sf.json.JSONObject;

public class HdfsHttpProxyClientTest extends HdfsProxyClient{
	private static Logger logger = Logger.getLogger(HdfsHttpProxyClientTest.class);
	private static String fileInfoUrl = "http://localhost/fileinfo";
	private static String fileFullName = "/home/beaver/Documents/test/hadoop/harry.txt";
//	private static String fileFullName = "/home/beaver/Documents/test/hadoop/test.txt";
	private static String urlPrefix = "http://localhost/uploaddata?fileName=";
	private static String hdfsPrefix = "hdfs://localhost:9000/test/";
	private static int UPLOAD_SIZE = 1024 * 1024;
	private static int UPLOAD_RETRY_TIMES = 16;
	private long UPLOAD_ERROR_SLEEP_TIME = 5000;
	private static String contentType = "application/octet-stream";
	private FileSystem coreSys=null;
	private static  Configuration conf = new Configuration();
	static{
		try {
			conf.set(BeaverCommonUtil.BEAVER_MODULE_TOKEN_CONF, BeaverCommonUtil.getAccessToken(HdfsProxyClient.class));
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public void uploadFileDataWithLength(String fileFullName, String url, int size){
		int totalSize = 0;
		String fileName = fileFullName.substring(fileFullName.lastIndexOf("/")+1);
		RandomAccessFile in;
		boolean doesUploadSueeccd = true;
		try {
			in = new RandomAccessFile(fileFullName,"r");
			long seekPos = 0;
			String json = BeaverUtils.doGet(fileInfoUrl + "?fileName=" + fileName);
			JSONObject jsonObject = JSONObject.fromObject(json);
			if(fileName.equals(jsonObject.get("file")) && !jsonObject.get("length").equals(-1)){
				 seekPos = Long.valueOf(jsonObject.get("length").toString());
			}
			in.seek(seekPos);
	        byte [] readBuf = new byte[UPLOAD_SIZE];
	        int readCount = 0;
	        int len = 0;
	        while(readCount < UPLOAD_SIZE && (len = in.read(readBuf, readCount, UPLOAD_SIZE - readCount)) != -1){
	        	readCount += len;
	 			if(readCount == UPLOAD_SIZE){
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

	@Test
	public void testUploadFileDataByBatch(){
		String fileName = fileFullName.substring(fileFullName.lastIndexOf("/")+1);
		String url = urlPrefix + fileName;
		try {
			for(int i = 0; i < 10; i++){
				logger.info("execute " + (i+1) + " upload");
				uploadFileDataWithLength(fileFullName,url,UPLOAD_SIZE);
			}		
			logger.info("upload data to HDFS succeed!");
			String hdfsData = getMd5ByString(readFromHdfs(fileFullName.substring(fileFullName.lastIndexOf("/")+1)));
			String localData = getMd5ByString(readFromLocal(fileFullName));
			Assert.assertEquals(hdfsData, localData);
		} catch (IOException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("upload data to server failed!");
		}
	}

	@Test
	public void testUploadFileData(){
		String fileName = fileFullName.substring(fileFullName.lastIndexOf("/")+1);
		String url = urlPrefix + fileName;
		try {
			doUploadFileData(fileFullName, url);
			logger.info("upload data to HDFS succeed!");
			String hdfsData = getMd5ByString(readFromHdfs(fileFullName.substring(fileFullName.lastIndexOf("/")+1)));
			String localData = getMd5ByString(readFromLocal(fileFullName));
			Assert.assertEquals(hdfsData, localData);
		} catch (IOException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("upload data to server failed!");
		}
	}

	public String readFromLocal(String localFile) throws FileNotFoundException{
		StringBuilder localData = new StringBuilder();
		FileInputStream stream = null;
		try {
			stream = new FileInputStream(localFile);
			int len = 0;
			byte[] writeBuf = new byte[1024];
			while((len = stream.read(writeBuf))!=-1){
				localData.append(new String(writeBuf, 0, len, "UTF-8"));
			}
			stream.close();
		}catch (IOException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("can not open local file!");
		}
		return localData.toString();
	}

	public String readFromHdfs(String filename) {
		Path path = new Path(hdfsPrefix + filename);
		StringBuilder fileData = new StringBuilder();
		int len = 0;
		try {
			coreSys=FileSystem.get(URI.create(hdfsPrefix), conf);
			FSDataInputStream dis = coreSys.open(path);
			byte[] writeBuf = new byte[1024];
			while((len = dis.read(writeBuf)) > 0){
	        	fileData.append(new String(writeBuf, 0, len, "UTF-8"));
	        }
	        dis.close();
	   	 	coreSys.close();
		} catch (IOException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("can not connect HDFS!");
		}
		return fileData.toString();
	}

	public static String getMd5ByString(String target) {
        return DigestUtils.md5Hex(target);
    }

	public static void main(String[] args) {
		HdfsHttpProxyClientTest hdfsHttpProxyTest = new HdfsHttpProxyClientTest();
//		hdfsHttpProxyTest.testUploadFileDataByBatch();
		hdfsHttpProxyTest.testUploadFileData();
	}
}
