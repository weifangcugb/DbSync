package com.cloudbeaver.client.fileUploader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import com.cloudbeaver.client.common.BeaverUtils;

public class DirInfo {
	private static Logger logger = Logger.getLogger(DirInfo.class);

	File dir = null;
	long miniChangeTime = 0;
	List<FileInfo> finfos = new ArrayList<FileInfo>();
	private String queryTime = null;
	private FileInfo UploadingFile = null;
	private String dirName = null;//this name is get from task server, don't change it

	private int FILE_UPLOAD_RETRY_TIMES = 16;

	private long FILE_UPLOAD_ERROR_SLEEP_TIME = 5000;

	public String getDirName() {
		return dirName;
	}

	public void setDirName(String dirName) {
		this.dirName = dirName;
	}

	public void setQueryTime(String touchTime){
		this.queryTime = touchTime;
	}

	public String getQueryTime() {
		return queryTime;
	}

	public File getDir() {
		return dir;
	}

	public void setDir(File dir) {
		this.dir = dir;
	}

	public String getUploadingFile(){
		return UploadingFile == null ? "" : UploadingFile.getFile().getName();
	}

	public DirInfo(String dirName, long miniChangeTime) throws IOException {
		this(dirName, new File(dirName), miniChangeTime);
	}

	public DirInfo(String dirName, String miniChangeTime) throws IOException {
		this(dirName, new File(dirName), BeaverUtils.hexTolong(miniChangeTime));
	}

	public DirInfo(String dirName, File dir, long miniChangeTime) throws IOException {
		if (dir.exists() && dir.isDirectory()) {
			this.dirName = dirName;
			this.dir = dir;
			this.miniChangeTime = miniChangeTime;
		}else {
			throw new IOException("dir is not exist or is not directory, dir:" + dir.getAbsolutePath());
		}
	}

	public String getMiniChangeTimeAsHexString(){
		String hex = BeaverUtils.longToHex(miniChangeTime);
		for (int i = 0; i < 16 - hex.length(); i++) {
			hex = "0" + hex;
		}
		return hex;
	}

	public void setMiniChangeTime(long miniChangeTime) {
		this.miniChangeTime = miniChangeTime;
	}

	public void listAndSortFiles() {
		finfos.clear();
		listAllLevelsFiles(dir);
		Collections.sort(finfos);
	}

	private void listAllLevelsFiles(File fatherDir) {
		File[] files = fatherDir.listFiles();
		for (File file : files) {
			if (file.isDirectory()) {
				listAllLevelsFiles(file);
			}else{
				long changeTime = file.lastModified();
				if (changeTime > miniChangeTime) {
					FileInfo finfo = new FileInfo(file, changeTime);
					finfos.add(finfo);
				}
			}
		}
	}

	public void uploadFiles() {
		for (FileInfo fileInfo : finfos) {
			setQueryTime((new Date()).toString());
			UploadingFile = fileInfo;

			String fileData = "";
			try {
				logger.info("start to read file, file:" + fileInfo.getFile().getAbsolutePath());
				fileData = fileInfo.getFileData();
				logger.info("finish read file, file:" + fileInfo.getFile().getAbsolutePath());
			} catch (IOException e) {
				BeaverUtils.PrintStackTrace(e);
				logger.error("read file data error, maybe disk block damaged, will jump this error file. file: " + fileInfo.getFile().getAbsolutePath() + "msg:" + e.getMessage());

				continue;
			}

			for (int i = 0; i < FILE_UPLOAD_RETRY_TIMES ; i++) {
				try {
					logger.info("start to upload file, file:" + fileInfo.getFile().getAbsolutePath());
					fileInfo.uploadFileData(fileInfo.getFile().getName(), fileData, BeaverUtils.longToHex(fileInfo.getModifyTime()), dirName);
					setMiniChangeTime(fileInfo.getModifyTime());
					logger.info("finish upload file, file:" + fileInfo.getFile().getAbsolutePath());

					break;
				} catch (IOException e) {
					BeaverUtils.PrintStackTrace(e);
					logger.error("upload file data error, will jump this error file. file: " + fileInfo.getFile().getAbsolutePath() + "msg:" + e.getMessage());

					BeaverUtils.sleep(FILE_UPLOAD_ERROR_SLEEP_TIME );
				}	
			}

			UploadingFile = null;
			BeaverUtils.sleep(200);
		}

		logger.info("one loop has finished, uploadFileNum:" + finfos.size());
	}
}
