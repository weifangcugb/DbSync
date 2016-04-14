package com.cloudbeaver.dbsync.fileLoader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

import com.cloudbeaver.dbsync.common.BeaverUtils;

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

	private void listAndSortFiles() {
		File[] files = dir.listFiles();
		for (File file : files) {
			long changeTime = file.lastModified();
			if (changeTime > miniChangeTime) {
				FileInfo finfo = new FileInfo(file, changeTime);
				finfos.add(finfo);
			}
		}
		Collections.sort(finfos);
	}

	private void uploadFiles() {
		for (FileInfo fileInfo : finfos) {
			try {
				fileInfo.uploadData();
			} catch (IOException e) {
				BeaverUtils.PrintStackTrace(e);
				logger.error("upload file error, msg:" + e.getMessage());
			}
		}
	}

	public void listSortUploadFiles() {
		listAndSortFiles();
		uploadFiles();
	}
}
