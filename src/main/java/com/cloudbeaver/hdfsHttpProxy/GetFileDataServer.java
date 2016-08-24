package com.cloudbeaver.hdfsHttpProxy;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
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

@WebServlet("/uploaddata")
public class GetFileDataServer extends HttpServlet{
	private static Logger logger = Logger.getLogger(GetFileDataServer.class);
//	public static int BUFFER_SIZE = 50;
	public static int BUFFER_SIZE = 512 * 1024;
	private static String rootPath = new String("hdfs://localhost:9000/test/");
	private static Map<String, FileInfoBean> fileInfoMap = new HashMap<String, FileInfoBean>();
    private FileSystem coreSys=null;
	private static  Configuration conf = new Configuration();
	{
		try {
			conf.set(BeaverCommonUtil.BEAVER_MODULE_TOKEN_CONF, BeaverCommonUtil.getAccessToken(HdfsHttpProxy.class));
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public void initFileSystemObject(String path){
		try {
			conf.setBoolean("dfs.support.append", true);
			coreSys=FileSystem.get(URI.create(rootPath), conf);
        } catch (IOException e) {
        	BeaverUtils.PrintStackTrace(e);
        	logger.error("init FileSystem failed:"+e.getLocalizedMessage());
        }
	}

	public Map<String, FileInfoBean> getFileInfo() {
		return fileInfoMap;
	}

	@Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp){
    	String filename = req.getParameter("filename");
    	String path = rootPath + filename;
    	initFileSystemObject(path);
    	ServletInputStream mRead;
		try {
			mRead = req.getInputStream();
			boolean doesFileExist = checkFileExist(path);
	    	if(!doesFileExist){
	    		if(!fileInfoMap.containsKey(filename)){
	    			createFile(mRead,path,filename);
	    		}
	    		else{
	    			logger.error(filename + " doesn't exist in hdfs but it is in fileInfoMap!");
	    		}
	    	}
	    	if(fileInfoMap.containsKey(filename)){
    			appendFile(mRead,path,filename);
    		}
    		else{
    			logger.error(filename + " exists in hdfs but it isn't in fileInfoMap!");
    		}
	    	coreSys.close();
	    	mRead.close();
		} catch (IOException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("upload data to HDFS failed!");
		}
    }

    public FileInfoBean updateFileInfoMap(String filename, byte[] buffer, int bufferSize, int offset) {
		FileInfoBean fileInfoBean = new FileInfoBean();
		fileInfoBean.setBuffer(buffer);
		fileInfoBean.setBufferSize(bufferSize);
		fileInfoBean.setOffset(offset);
		fileInfoMap.put(filename, fileInfoBean);
		return fileInfoBean;
	}

    public void appendFile(ServletInputStream mRead, String path, String filename) {
    	Path hdfsPath = new Path(path);
    	logger.info(hdfsPath);
    	int readCount = 0;
    	int len = 0;
    	FileInfoBean fileInfoBean = fileInfoMap.get(filename);
    	try {
    		FSDataOutputStream fsout = coreSys.append(hdfsPath);
    		if(fileInfoBean.bufferSize > 0){
    			fsout.write(fileInfoBean.buffer, 0, fileInfoBean.bufferSize);
    			logger.info("upload data:" + fileInfoBean.getBuffer());
 				fileInfoBean = updateFileInfoMap(filename, new byte[BUFFER_SIZE], 0, fileInfoBean.offset + fileInfoBean.bufferSize);
    		}
	 		while(readCount < BUFFER_SIZE && (len = mRead.read(fileInfoBean.buffer, readCount, BUFFER_SIZE - readCount)) != -1){
	 			readCount += len;
	 			fileInfoBean = updateFileInfoMap(filename, fileInfoBean.buffer, readCount, fileInfoBean.offset);
	 			if(readCount == BUFFER_SIZE){
	 				fsout.write(fileInfoBean.buffer, 0, readCount);
	 				logger.info("upload data:" + fileInfoBean.getBuffer());
	 				fileInfoBean = updateFileInfoMap(filename, new byte[BUFFER_SIZE], 0, fileInfoBean.offset + readCount);
	 				readCount = 0;
	 			}
	    	}
	 		if(readCount > 0){
	 			fsout.write(fileInfoBean.buffer, 0, readCount);
	 			logger.info("upload data:" + fileInfoBean.getBuffer());
 				updateFileInfoMap(filename, new byte[BUFFER_SIZE], 0, fileInfoBean.offset + readCount);
	 		}
	 		fsout.close();
		} catch (IOException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("append data to HDFS failed!");
		}
	}

    public void createFile(ServletInputStream mRead, String path, String filename) {
    	Path hdfsPath = new Path(path);
    	logger.info(hdfsPath);
	    FSDataOutputStream fsout = null;
    	int readCount = 0;
    	int len = 0;
    	FileInfoBean fileInfoBean = new FileInfoBean();
    	fileInfoMap.put(filename, fileInfoBean);
    	try {
			fsout = coreSys.create(hdfsPath);
	 		readCount = mRead.read(fileInfoBean.buffer);
	 		while(readCount != -1 && readCount < BUFFER_SIZE) {
	 			logger.info("read, readCount:" + readCount);
	 			len = mRead.read(fileInfoBean.buffer, readCount, BUFFER_SIZE - readCount);
	 			if(len != -1){
	 				readCount += len;
	 				fileInfoBean = updateFileInfoMap(filename, fileInfoBean.buffer, readCount, fileInfoBean.offset);
	 			}
	 			else{
	 				break;
	 			}
	 		}
	 		if(readCount == -1){
	 			logger.info("readCount :" + readCount);
	 			return;
	 		}
	 		fsout.write(fileInfoBean.buffer, 0, readCount);
	 		logger.info("upload data:" + fileInfoBean.getBuffer());
			fileInfoBean = updateFileInfoMap(filename, new byte[BUFFER_SIZE], 0, fileInfoBean.offset + readCount);
	        fsout.close();
		} catch (IOException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("create file in HDFS failed!");
		}
	}

	public boolean checkFileExist(String path) throws IOException {
        Path f = new Path(path);
        return coreSys.exists(f);
    }
}