package com.cloudbeaver.hadoopDFSClient;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
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
import com.cloudbeaver.hdfsHttpProxy.HdfsClient;

public class HdfsHttpProxyTest extends HdfsClient{
	private static Logger logger = Logger.getLogger(HdfsHttpProxyTest.class);
	private static String fileFullName = "/home/beaver/Documents/test/hadoop/harry.txt";
//	private static String filename = "/home/beaver/Documents/test/hadoop/test.txt";
	private static String urlPrefix = "http://localhost:8811/uploaddata?fileName=";
//	private static String urlPrefix = "http://localhost/?filename=";
	private static String hdfsPrefix = "hdfs://localhost:9000/test/";
//	private static String fileInfoUrl = "http://localhost:8090/fileinfo";
	private static int UPLOAD_SIZE = 1024 * 1024;
//	private static int UPLOAD_SIZE = 100;
	private FileSystem coreSys=null;
	private  Configuration conf = new Configuration();
	{
		try {
			conf.set(BeaverCommonUtil.BEAVER_MODULE_TOKEN_CONF, BeaverCommonUtil.getAccessToken(DFSClientTest.class));
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	@Test
	public void testUploadFileDataByBatch(){
		String fileName = fileFullName.substring(fileFullName.lastIndexOf("/")+1);
		String url = urlPrefix + fileName;
		EXCEPTION_TEST_MODE = false;
		try {
			for(int i = 0; i < 10; i++){
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
		EXCEPTION_TEST_MODE = false;
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
		HdfsHttpProxyTest hdfsHttpProxyTest = new HdfsHttpProxyTest();
		hdfsHttpProxyTest.testUploadFileDataByBatch();
//		hdfsHttpProxyTest.testUploadFileData();
	}
}
