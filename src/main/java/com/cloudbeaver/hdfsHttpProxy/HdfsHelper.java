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
	private static  Configuration conf = new Configuration();
	static {
		try {
			conf.set(BeaverCommonUtil.BEAVER_MODULE_TOKEN_CONF, BeaverCommonUtil.getAccessToken(HdfsClient.class));
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public static void writeFile(String filename, byte[] buffer, int readCount) throws IOException {
		FileSystem fileSystem = FileSystem.get(URI.create(rootPath), conf);
		Path fileFullName = new Path(rootPath + filename);
		FSDataOutputStream fsOut = fileSystem.exists(fileFullName) ? fileSystem.append(fileFullName) : fileSystem.create(fileFullName) ;
		fsOut.write(buffer, 0, readCount);
		fsOut.flush();
		fsOut.close();
	}

	public static long getFileLength(String fileName) throws IOException {
		FileSystem fileSystem = FileSystem.get(URI.create(rootPath), conf);
		Path fileFullName = new Path(rootPath + fileName);
		while(true){
			try{
				if (fileSystem.exists(fileFullName)) {
					FSDataOutputStream fsOut = fileSystem.append(fileFullName);
					fsOut.close();
					return fileSystem.getFileStatus(fileFullName).getLen();
				}else{
					return 0;
				}
			}catch (IOException e) {
				BeaverUtils.printLogExceptionAndSleep(e, "got exception when open a stream", 5000);
			}
		}
	}
}
