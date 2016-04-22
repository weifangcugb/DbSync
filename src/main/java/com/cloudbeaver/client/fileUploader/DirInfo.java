package com.cloudbeaver.client.fileUploader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

import com.cloudbeaver.client.common.BeaverUtils;

public class DirInfo {
	private static Logger logger = Logger.getLogger(DirInfo.class);

	File dir = null;
	long miniChangeTime = 0;
	List<FileInfo> finfos = new ArrayList<FileInfo>();

	public DirInfo(String dirName, long miniChangeTime) throws Exception {
		this(new File(dirName), miniChangeTime);
	}

	public DirInfo(File dir, long miniChangeTime) throws Exception {
		if (dir.exists() && dir.isDirectory()) {
			this.dir = dir;
			this.miniChangeTime = miniChangeTime;
		}else {
			throw new Exception("dir is not exist or is not directory, dir:" + dir.getAbsolutePath());
		}
	}

	public long getMiniChangeTime() {
		return miniChangeTime;
	}

	public void setMiniChangeTime(long miniChangeTime) {
		this.miniChangeTime = miniChangeTime;
	}

	public void listAndSortFiles() {
		finfos.clear();

		File[] files = dir.listFiles();
		for (File file : files) {
			long changeTime = file.lastModified();
//			logger.debug("file:" + file.getName() + " changeTime:" + changeTime + " mini:" + miniChangeTime);
			if (changeTime > miniChangeTime) {
				FileInfo finfo = new FileInfo(file, changeTime);
				finfos.add(finfo);
			}
		}

		Collections.sort(finfos);
	}

	public void uploadFiles() {
		for (FileInfo fileInfo : finfos) {
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

//			TODO: resend logic
			try {
				logger.info("start to upload file, file:" + fileInfo.getFile().getAbsolutePath());
				fileInfo.uploadFileData(fileInfo.getFile().getName(), fileData, fileInfo.getModifyTime());
				setMiniChangeTime(fileInfo.getModifyTime());
				logger.info("finish upload file, file:" + fileInfo.getFile().getAbsolutePath());
			} catch (IOException e) {
				BeaverUtils.PrintStackTrace(e);
				logger.error("upload file data error, will jump this error file. file: " + fileInfo.getFile().getAbsolutePath() + "msg:" + e.getMessage());
			}

			BeaverUtils.sleep(200);
		}
	}
}
