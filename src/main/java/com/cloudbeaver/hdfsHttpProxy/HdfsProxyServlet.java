package com.cloudbeaver.hdfsHttpProxy;

import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;
import com.cloudbeaver.client.common.BeaverUtils;

@WebServlet("/uploaddata")
public class HdfsProxyServlet extends HttpServlet{
	private static Logger logger = Logger.getLogger(HdfsProxyServlet.class);
	protected boolean EXCEPTION_TEST_MODE = false;
	public static int BUFFER_SIZE = FileInfoBean.BUFFER_SIZE;
	private HdfsHelper hdfsHelper = new HdfsHelper();
	private static Map<String, FileInfoBean> fileInfoMap = new Hashtable<String, FileInfoBean>();

	public static Map<String, FileInfoBean> getFileInfo() {
		return fileInfoMap;
	}

	@Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException{
    	String filename = req.getParameter("filename");

    	int bufferSize = 64 * 1024 * 1024;
    	byte[] buffer = new byte[bufferSize];
    	ServletInputStream servletInputStream = req.getInputStream();
    	int readNumTillNow = 0, len = 0;
    	while((len = servletInputStream.read(buffer, readNumTillNow, bufferSize - readNumTillNow)) != -1){
			readNumTillNow += len;
			if(readNumTillNow == BUFFER_SIZE){
				HdfsHelper.writeFile(filename, buffer, readNumTillNow);
				logger.info("upload data, file:" + filename + " size:" + readNumTillNow);

//				clear buffer and read next block
				readNumTillNow = 0;
				BeaverUtils.clearByteArray(buffer);
			}
		}

//    	read to the end of the file
    	if (readNumTillNow > 0) {
    		HdfsHelper.writeFile(filename, buffer, readNumTillNow);
		}
    }

    protected void doPost2(HttpServletRequest req, HttpServletResponse resp){
    	String filename = req.getParameter("filename");
    	hdfsHelper.initFileSystemObject();
    	ServletInputStream mRead;
		try {
			mRead = req.getInputStream();
			boolean doesFileExist = hdfsHelper.checkFileExist(filename);
	    	if(!doesFileExist){
	    		if(!fileInfoMap.containsKey(filename)){
	    			createFile(mRead, filename);
	    		}
	    		else{
	    			logger.error(filename + " doesn't exist in hdfs but it is in fileInfoMap!");
	    		}
	    	}
	    	if(fileInfoMap.containsKey(filename)){
    			appendFile(mRead, filename);
    		}
    		else{
    			logger.error(filename + " exists in hdfs but it isn't in fileInfoMap!");
    		}
	    	mRead.close();
	    	hdfsHelper.tearDownConnection();
		} catch (IOException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("upload data to HDFS failed!");
		}
    }

    public FileInfoBean updateFileInfoMap(String filename, byte[] buffer, int bufferSize, long offset) {
		FileInfoBean fileInfoBean = new FileInfoBean();
		fileInfoBean.setBuffer(buffer);
		fileInfoBean.setBufferSize(bufferSize);
		fileInfoBean.setOffset(offset);
		fileInfoMap.put(filename, fileInfoBean);
		return fileInfoBean;
	}

    public FileInfoBean uploadExistedData(FileInfoBean fileInfoBean, ServletInputStream mRead, String filename) {
    	int len = 0;
    	int readCount = fileInfoBean.bufferSize;
		try {
			while(readCount < BUFFER_SIZE && (len = mRead.read(fileInfoBean.buffer, readCount, BUFFER_SIZE - readCount)) != -1){
				readCount += len;
				if(readCount == BUFFER_SIZE){
					hdfsHelper.appendFile(filename, fileInfoBean.buffer, 0, readCount);
					logger.info("upload data:" + fileInfoBean.getBuffer());
					fileInfoBean = updateFileInfoMap(filename, new byte[BUFFER_SIZE], 0, fileInfoBean.offset + readCount - fileInfoBean.bufferSize);
					readCount = 0;
					break;
				}
			}
			if(readCount > 0){
				hdfsHelper.appendFile(filename, fileInfoBean.buffer, 0, readCount);
				logger.info("upload data:" + fileInfoBean.getBuffer());
				if(readCount == BUFFER_SIZE){
					fileInfoBean = updateFileInfoMap(filename, new byte[BUFFER_SIZE], 0, fileInfoBean.offset);
				}
				else{
					fileInfoBean = updateFileInfoMap(filename, new byte[BUFFER_SIZE], 0, fileInfoBean.offset + readCount - fileInfoBean.bufferSize);
				}
			}
		} catch (IOException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("append existed data to HDFS failed!");
		}
		return fileInfoBean;
		
	}

    public void appendFile(ServletInputStream mRead, String filename) {
    	int readCount = 0;
    	int len = 0;
    	FileInfoBean fileInfoBean = fileInfoMap.get(filename);
    	try {
    		if(fileInfoBean.bufferSize > 0){
    			fileInfoBean = uploadExistedData(fileInfoBean, mRead, filename);
    		}
	 		while(readCount < BUFFER_SIZE && (len = mRead.read(fileInfoBean.buffer, readCount, BUFFER_SIZE - readCount)) != -1){
//	 		while(readCount < BUFFER_SIZE && (len = mRead.read(fileInfoBean.buffer, readCount, 25)) != -1){
	 			readCount += len;
	 			fileInfoBean = updateFileInfoMap(filename, fileInfoBean.buffer, readCount, fileInfoBean.offset);

	 			if(EXCEPTION_TEST_MODE){
	 				updateFileInfoMap(filename, fileInfoBean.buffer, readCount, fileInfoBean.offset + readCount);
	 				return ;
	 			}

	 			if(readCount == BUFFER_SIZE){
	 				hdfsHelper.appendFile(filename, fileInfoBean.buffer, 0, readCount);
	 				logger.info("upload data:" + fileInfoBean.getBuffer());
	 				fileInfoBean = updateFileInfoMap(filename, new byte[BUFFER_SIZE], 0, fileInfoBean.offset + readCount);
	 				readCount = 0;
	 			}
	    	}
	 		if(readCount > 0){
	 			hdfsHelper.appendFile(filename, fileInfoBean.buffer, 0, readCount);
	 			logger.info("upload data:" + fileInfoBean.getBuffer());
 				updateFileInfoMap(filename, new byte[BUFFER_SIZE], 0, fileInfoBean.offset + readCount);
	 		}
		} catch (IOException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("append data to HDFS failed!");
		}
	}

    public void createFile(ServletInputStream mRead, String filename) {
    	int readCount = 0;
    	int len = 0;
    	FileInfoBean fileInfoBean = new FileInfoBean();
    	fileInfoMap.put(filename, fileInfoBean);
    	try {
	 		readCount = mRead.read(fileInfoBean.buffer);
	 		while(readCount != -1 && readCount < BUFFER_SIZE) {
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
	 			logger.info("read, readCount:" + readCount);
	 			return;
	 		}
	 		hdfsHelper.createFile(filename, fileInfoBean.buffer, 0, readCount);
	 		logger.info("upload data:" + fileInfoBean.getBuffer());
			fileInfoBean = updateFileInfoMap(filename, new byte[BUFFER_SIZE], 0, fileInfoBean.offset + readCount);
		} catch (IOException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("create file in HDFS failed!");
		}
	}
}