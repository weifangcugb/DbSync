package com.cloudbeaver.client.fileLoader;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLConnection;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

import com.cloudbeaver.client.common.BeaverUtils;

public class FileInfo implements Comparable<FileInfo>{
	private static Logger logger = Logger.getLogger(DirInfo.class);

	private File file = null;
	private long modifyTime = 0;

	public FileInfo(File file, long changeTime) {
		this.file = file;
		this.modifyTime = changeTime;
	}

	@Override
	public int compareTo(FileInfo o) {
		return this.modifyTime > o.modifyTime ? 1: ((this.modifyTime == o.modifyTime) ? 0 : -1);
	}

	public File getFile() {
		return file;
	}

	public void setFile(File file) {
		this.file = file;
	}

	public long getModifyTime() {
		return modifyTime;
	}

	public void setModifyTime(long modifyTime) {
		this.modifyTime = modifyTime;
	}

	private void uploadFileData(String fileName, String fileData, long lastModified) throws IOException{
		/*
		 * e.g.
		 * [[{\"hdfs_prison\":\"1\",\"hdfs_db\":\"DocumentDB\",\"hdfs_table\":\"da_jbxx\",\"id\":\"337178\"
		 */
		String uploadData = "[[{\"hdfs_prison\":\"" + FileUploader.getClientId() + "\",\"hdfs_db\":\"files\",\"hdfs_table\":\"pics\",\"pic_name\":\"" 
							+ fileName + "\", \"pic_data\":\"" + fileData + "\", \"xgsj\":\"" +  lastModified + "\"}]]";

		uploadData = uploadData.replaceAll("\"", "\\\\\"");
        String flumeJson = "[{ \"headers\" : {}, \"body\" : \"" + uploadData + "\" }]";

        BeaverUtils.doPost(FileUploader.getFlumeServer(), flumeJson);
	}

	private String getFileData() throws IOException {
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

		byte[] datas = bout.toByteArray();
		if (datas.length == 0) {
			logger.info("one empty pic file, file:" + file.getAbsolutePath());
			return "";
		}else {
			return new String(Base64.encodeBase64(datas));
		}
	}

	public void uploadData() throws IOException {
		logger.info("start to upload pic, file:" + file.getAbsolutePath());
		String fileData = getFileData();
		uploadFileData(file.getName(),fileData, modifyTime);
		logger.info("finish upload pic, file:" + file.getAbsolutePath());
	}
}
