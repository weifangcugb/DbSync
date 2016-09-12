package com.cloudbeaver.hadoopDFSClient;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.BeaverCommonUtil;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.client.fileUploader.FileUploader;

public class DFSClientTest {
	private static Logger logger = Logger.getLogger(DFSClientTest.class);
//	private static File picDir = new File("src/resources/fileUploaderTestPics");
	private static File picDir = new File("/home/beaver/Documents/test/hadoop/hdfsClient/");
	private static File[] files = picDir.listFiles();
	private static String rootPath=new String("hdfs://localhost:9000/");
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

	@Before
    public void initFileSystemObject(){
      try {
    	  conf.set("dfs.block.size", "1048576");
    	  coreSys=FileSystem.get(URI.create(rootPath), conf);
       } catch (IOException e) {
           logger.error("init FileSystem failed:"+e.getLocalizedMessage());
       }
    }

    public String readFile(String path) throws Exception{
    	Path hdfsPath = new Path(path);
   	 	FSDataInputStream fsin = coreSys.open(hdfsPath);
        byte[] writeBuf = new byte[1024];
        int len = 0;
        String fileData = "";
        while((len = fsin.read(writeBuf)) > 0){
        	fileData += new String(writeBuf, 0, len, "UTF-8");
        }
        fsin.close();
   	 	logger.info("file reading completes！");
   	 	return fileData;
    }

    public void createFile(String path, File file) throws Exception{
 		Path hdfsPath = new Path(path);
 		logger.info(hdfsPath);
 		FSDataOutputStream fsout = coreSys.create(hdfsPath);
 		BufferedOutputStream bout = new BufferedOutputStream(fsout);
 		String fileData = getFileData(file);
		bout.write(fileData.getBytes(), 0, fileData.getBytes().length);	
        bout.close();
        fsout.close();
        logger.info("file creation succeeds！");
    }

    public String getFileData(File file) throws IOException {
		long fileSize = file.length();
		byte[] datas;
		if (fileSize == 0) {
			logger.info("one empty pic file, file:" + file.getAbsolutePath());
			return "";
		}else{
			FileInputStream fin = new FileInputStream(file);
			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			byte[] buffer = new byte[4096];
			while(true){
				int len = fin.read(buffer);
				if (len == -1) {
					break;
				}
				bout.write(buffer, 0, len);
			}
			datas = bout.toByteArray();
		}
		return new String(Base64.encodeBase64(datas));
	}

    public static String getMd5ByFile(File file) throws FileNotFoundException {  
        String value = null;  
        FileInputStream in = new FileInputStream(file);
	    try {  
	        MappedByteBuffer byteBuffer = in.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, file.length());  
	        MessageDigest md5 = MessageDigest.getInstance("MD5");  
	        md5.update(byteBuffer);  
	        BigInteger bi = new BigInteger(1, md5.digest());  
	        value = bi.toString(16);  
	    } catch (Exception e) {  
	        e.printStackTrace();  
	    } finally {  
	            if(null != in) {  
	                try {  
	                in.close();  
	            } catch (IOException e) {  
	                e.printStackTrace();  
	            }  
	        }  
	    }  
	    return value;  
    }

    public static String getMd5ByString(String target) {
        return DigestUtils.md5Hex(target);
    }

    @Test
    public void testReadFile() throws Exception{
    	String path = rootPath + "hdfsupload/pics/";
    	Path hdfsPath = new Path(path);
 		FileStatus [] inputFiles = coreSys.listStatus(hdfsPath);
 		logger.info("number of files :" + inputFiles.length);
   	 	for(int i = 0; i < inputFiles.length; i++){
   	 		String filename = inputFiles[i].getPath().getName();
   	 		logger.info("file path :" + path + filename);
   	 		logger.info("block size :" + inputFiles[i].getBlockSize());
   	 		logger.info("file size :" + inputFiles[i].getLen());
   	 		String fileData = readFile(path + filename);
//   	        logger.info(fileData);
   	 		String hdfsMd5 = getMd5ByString(fileData);
   	 		String localMd5 = getMd5ByString(getFileData(new File(picDir + "/" + filename)));
   	 		Assert.assertEquals(hdfsMd5, localMd5);
   	 	}
    }

    @Test
    public void testCreateFile() throws Exception{
    	String prefix = rootPath + "hdfsupload/pics/";
    	for (File file : files) {
    		String filename = file.getName();			
			createFile(prefix + filename, file);
    	}
    }

	public static void main(String[] args) throws Exception {
		DFSClientTest dfsClientTest = new DFSClientTest();
		dfsClientTest.initFileSystemObject();
		dfsClientTest.testCreateFile();
		dfsClientTest.testReadFile();
	}
}
