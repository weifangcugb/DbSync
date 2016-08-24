package com.cloudbeaver.client.fileUploader;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.client.common.CommonUploader;

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

	public void uploadFileData(String fileName, String fileData, String changeTime, String dirPath) throws IOException {
		/*
		 * e.g.
		 * [[{\"hdfs_client\":\"1\",\"hdfs_db\":\"DocumentDB\",\"hdfs_table\":\"da_jbxx\",\"id\":\"337178\"
		 */
		String uploadData = "[{\"hdfs_prison\":\"" + FileUploader.getPrisonId()
				+ "\", \"hdfs_db\":\"" + CommonUploader.TASK_FILEDB_NAME + "\",\"hdfs_table\":\"" + dirPath + "\",\"file_name\":\"" + fileName 
				+ "\", \"file_data\":\"" + fileData + "\", \"xgsj\":\"" +  changeTime + "\"}]";

        String flumeJson;
		try {
			flumeJson = BeaverUtils.compressAndFormatFlumeHttp(uploadData);
		} catch (IOException e) {
//			this is impossible unless system memory has some error, as I think
			BeaverUtils.PrintStackTrace(e);
			logger.error("write gzip stream to memory error, msg:" + e.getMessage());
			throw e;
		}

        BeaverUtils.doPost(FileUploader.getFlumeServer(), flumeJson);
	}

	public String getFileData() throws IOException {
		long fileSize = file.length();

		byte[] datas;
		if (fileSize == 0) {
			logger.info("one empty pic file, file:" + file.getAbsolutePath());
			return "";
		}else if (fileSize <= FileUploader.LARGE_PIC_SIZE_BARRIER) {
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
		}else {
			if (BeaverUtils.fileIsPics(file.getName().toLowerCase())) {
//				resize a big jpg picture
				datas = BeaverUtils.resizePic(file, (int)fileSize, FileUploader.LARGE_PIC_SIZE_BARRIER);

//				FileOutputStream fout = new FileOutputStream(new File(file.getAbsolutePath() + ".small"));
//				fout.write(datas);
//				fout.close();
    		}else {
    			throw new IOException("this non-pic file is too large to be upload, fileSize:" + fileSize + " file:" + file.getAbsolutePath());
    		}
		}

		return new String(Base64.encodeBase64(datas));
	}
}
