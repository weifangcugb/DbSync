package com.cloudbeaver.hdfsHttpProxy;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.BeaverCommonUtil;
import org.apache.log4j.Logger;

import com.cloudbeaver.client.common.BeaverUtils;

public class HdfsHelper {
	private static Logger logger = Logger.getLogger(HdfsHelper.class);

	private static String rootPath = new String("hdfs://localhost:9000/test/");
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

	public void initFileSystemObject(){
		try {
			conf.setBoolean("dfs.support.append", true);
			coreSys=FileSystem.get(URI.create(rootPath), conf);
        } catch (IOException e) {
        	BeaverUtils.PrintStackTrace(e);
        	logger.error("init FileSystem failed:"+e.getLocalizedMessage());
        }
	}

	public void tearDownConnection() throws IOException {
		coreSys.close();
	}

	public void createFile(String filename, byte [] buffer, int startPos, int readCount){
		Path path = new Path(rootPath + filename);
		try {
			FSDataOutputStream fsout = coreSys.create(path);
			fsout.write(buffer, startPos, readCount);
			fsout.close();
		} catch (IOException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("create file in HDFS failed!");
		}
	}

	public void appendFile(String filename, byte [] buffer, int startPos, int readCount){
		Path path = new Path(rootPath + filename);
		try {
			FSDataOutputStream fsout = coreSys.append(path);
			fsout.write(buffer, startPos, readCount);
			fsout.close();
		} catch (IOException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("append data to HDFS failed!");
		}
	}

	public boolean checkFileExist(String filename) throws IOException {
        Path path = new Path(rootPath + filename);
        return coreSys.exists(path);
    }
}
