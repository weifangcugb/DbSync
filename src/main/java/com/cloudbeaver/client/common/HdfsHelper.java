package com.cloudbeaver.client.common;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.BeaverCommonUtil;
import org.apache.log4j.Logger;
import com.cloudbeaver.hdfsHttpProxy.HdfsProxyClient;

public class HdfsHelper {
	private static Logger logger = Logger.getLogger(HdfsHelper.class);

	private static String rootPath = new String("hdfs://localhost:9000/test/");
	private static  Configuration conf = new Configuration();
	static {
		try {
			conf.set(BeaverCommonUtil.BEAVER_MODULE_TOKEN_CONF, BeaverCommonUtil.getAccessToken(HdfsProxyClient.class));
			logger.info("HDFS conf has been set");
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public static void writeFile(String path, byte[] buffer, int readCount) throws IOException {
		logger.info("start to write hdfs file, path:" + path + " len:" + readCount);
		FileSystem fileSystem = FileSystem.get(URI.create(rootPath), conf);
		Path fileFullName = new Path(path);
		FSDataOutputStream fsOut = fileSystem.exists(fileFullName) ? fileSystem.append(fileFullName) : fileSystem.create(fileFullName) ;
		fsOut.write(buffer, 0, readCount);
		fsOut.flush();
		fsOut.close();
		logger.info("finish writing hdfs file, path:" + path + " len:" + readCount);
	}

	public static long getFileLength(String path) throws IOException {
		FileSystem fileSystem = FileSystem.get(URI.create(rootPath), conf);
		Path fileFullName = new Path(path);
		while(true){
			try{
				if (fileSystem.exists(fileFullName)) {
					FSDataOutputStream fsOut = fileSystem.append(fileFullName);
					fsOut.close();
					return fileSystem.getFileStatus(fileFullName).getLen();
				}else{
					return -1;
				}
			}catch (IOException e) {
				BeaverUtils.printLogExceptionAndSleep(e, "got exception when open a stream", 5000);
			}
		}
	}

	public static String getRealPathWithTableId(String tableId) {
		return tableId;
	}
}