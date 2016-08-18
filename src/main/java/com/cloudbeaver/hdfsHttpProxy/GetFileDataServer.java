package com.cloudbeaver.hdfsHttpProxy;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.BeaverCommonUtil;
import org.apache.log4j.Logger;
import com.cloudbeaver.client.common.BeaverUtils;
import com.sun.org.apache.xml.internal.resolver.helpers.PublicId;
import com.sun.scenario.effect.Offset;

@WebServlet("/filedata")
public class GetFileDataServer extends HttpServlet{
	private static Logger logger = Logger.getLogger(GetFileDataServer.class);
	private static String rootPath = new String("hdfs://localhost:9000/test");
	private static int LEN_PER_TIME = 1024 * 1024;
	private static int BUFFER_SIZE = 100;
    private static FileSystem coreSys=null;
	private static  Configuration conf = new Configuration();
	{
		try {
			conf.set(BeaverCommonUtil.BEAVER_MODULE_TOKEN_CONF, BeaverCommonUtil.getAccessToken(HdfsHttpProxy.class));
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public static void initFileSystemObject(){
		try {
			conf.setBoolean("dfs.support.append", true);
			coreSys=FileSystem.get(URI.create(rootPath), conf);
        } catch (IOException e) {
        	logger.error("init FileSystem failed:"+e.getLocalizedMessage());
        }
	}

	@Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException{
    	initFileSystemObject();
    	ServletInputStream mRead = req.getInputStream();
    	byte[] buf = new byte[BUFFER_SIZE];
    	int len;
    	while((len = mRead.read(buf)) != -1){    		
			startUploadFileToHdfs(buf,len);
    	}
    	coreSys.close();
    }

    public void startUploadFileToHdfs(byte[] filedata, int len) throws IOException {
		try {
    		boolean b = checkFileExist(rootPath);
    		if(!b){
    			createFile(rootPath, filedata, len);
    		}
    		else{
    			appendFile(rootPath, filedata, len);
    		}
		} catch (Exception e) {
			BeaverUtils.PrintStackTrace(e);
			logger.fatal("upload to HDFS failed!");
		}
	}

	public void createFile(String path, byte[] fileData, int len) throws IOException{
 		Path hdfsPath = new Path(path);
 		logger.info(hdfsPath);
 		FSDataOutputStream fsout = coreSys.create(hdfsPath);
 		BufferedOutputStream bout = new BufferedOutputStream(fsout);
// 		System.out.println("len = " + len);
 		int offset = 0;
 		while(offset < len){
 			if(len - offset < LEN_PER_TIME){
 				bout.write(fileData, offset, len - offset);
 				offset += (len - offset);
 			}
 			else{
 				bout.write(fileData, offset, LEN_PER_TIME);
 	 			offset += LEN_PER_TIME;
 			} 			
 		}
// 		System.out.println("offset = " + offset);
        bout.close();
        fsout.close();
        logger.info("upload file data succeeds！");
    }

	public void appendFile(String path, byte[] fileData, int len) throws Exception{
 		Path hdfsPath = new Path(path);
 		logger.info(hdfsPath);
 		FSDataOutputStream fsout = coreSys.append(hdfsPath);
 		BufferedOutputStream bout = new BufferedOutputStream(fsout);
// 		System.out.println("len = " + len);
 		int offset = 0;
 		while(offset < len){
 			if(len - offset < LEN_PER_TIME){
 				bout.write(fileData, offset, len - offset);
 				offset += (len - offset);
 			}
 			else{
 				bout.write(fileData, offset, LEN_PER_TIME);
 	 			offset += LEN_PER_TIME;
 			} 			
 		}
// 		System.out.println("offset = " + offset);
        bout.close();
        fsout.close();
        logger.info("upload file data succeeds！");
    }

	public boolean checkFileExist(String path) throws IOException {     
        Path f = new Path(path);
        return coreSys.exists(f);
    }
}