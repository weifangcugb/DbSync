package com.cloudbeaver.hdfsHttpProxy;

import java.io.BufferedOutputStream;
import java.io.IOException;
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

@WebServlet("/uploaddata")
public class GetFileDataServer extends HttpServlet{
	private static Logger logger = Logger.getLogger(GetFileDataServer.class);
	private static String rootPath = new String("hdfs://localhost:9000/test/");
	private static int LEN_PER_TIME = 1024 * 1024;
	private static int BUFFER_SIZE = 100;
    private FileSystem coreSys=null;
    private Path hdfsPath = null;
    private FSDataOutputStream fsout = null;
	private BufferedOutputStream bout = null;
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
			hdfsPath = new Path(path);
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
    	String filename = req.getParameter("filename");
    	String path = rootPath + filename;
    	initFileSystemObject(path);
    	ServletInputStream mRead = req.getInputStream();
    	boolean b = checkFileExist(path);
    	if(b){
    		appendFile(mRead);
    	}
    	else{
    		createFile(mRead);
    	}
    	bout.close();
        fsout.close();
    	coreSys.close();
    	mRead.close();
    }

    public void appendFile(ServletInputStream mRead) {
    	byte[] buf = new byte[BUFFER_SIZE];
    	int readCount = 0;
    	int len = 0;
    	try {
			fsout = coreSys.append(hdfsPath);
			bout = new BufferedOutputStream(fsout);
	 		while((readCount = mRead.read(buf)) != -1){
	 			while(readCount < BUFFER_SIZE) {
	     			len = mRead.read(buf, readCount, BUFFER_SIZE - readCount);
	     			if(len != -1){
	     				readCount += len;
	     			}
	     			else{
	     				break;
	     			}
	     		}
	    		writeFile(buf,readCount);
	    	}
		} catch (IOException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("append data failed!");
		}
	}

    public void createFile(ServletInputStream mRead) {
    	byte[] buf = new byte[BUFFER_SIZE];
    	int readCount = 0;
    	int len = 0;
    	try {
			fsout = coreSys.create(hdfsPath);
			bout = new BufferedOutputStream(fsout);
	 		readCount = mRead.read(buf);
	 		while(readCount != -1 && readCount < BUFFER_SIZE) {
	 			len = mRead.read(buf, readCount, BUFFER_SIZE - readCount);
	 			if(len != -1){
	 				readCount += len;
	 			}
	 			else{
	 				break;
	 			}
	 		}
			writeFile(buf,readCount);
			bout.close();
	        fsout.close();

			fsout = coreSys.append(hdfsPath);
	 		bout = new BufferedOutputStream(fsout);
	 		while((readCount = mRead.read(buf)) != -1){
	 			while(readCount < BUFFER_SIZE) {
	     			len = mRead.read(buf, readCount, BUFFER_SIZE - readCount);
	     			if(len != -1){
	     				readCount += len;
	     			}
	     			else{
	     				break;
	     			}
	     		}
	    		writeFile(buf,readCount);
	    	}
		} catch (IOException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("create file failed!");
		}
	}

	public void writeFile(byte[] fileData, int len) throws IOException{
 		logger.info(hdfsPath);
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
        logger.info("upload file data succeeds！");
    }

	public boolean checkFileExist(String path) throws IOException {
        Path f = new Path(path);
        return coreSys.exists(f);
    }
}