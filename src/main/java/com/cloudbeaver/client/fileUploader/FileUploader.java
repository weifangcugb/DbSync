package com.cloudbeaver.client.fileUploader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.log4j.Logger;

import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.client.common.FixedNumThreadPool;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FileUploader extends FixedNumThreadPool {
    private static Logger logger = Logger.getLogger(FileUploader.class);
	private static final String PIC_DIRECTORY_NAME = "db.DocumentFiles.url";
	private static final String FLUME_SERVER_URL = "flume-server.url";
	private static final String CLIENT_ID = "clientid";
	private static final boolean USE_REMOTE_DIRS = false;

	private static String flumeServer = "192.168.1.109:9092";
	private static String clientId = "1";

	private List<DirInfo> dirInfos = new ArrayList<DirInfo>();
	private String threadName = null;
	private String confName = "DbSyncClient.conf";

	public static String getFlumeServer() {
		return flumeServer;
	}

	public static void setFlumeServer(String flumeServer) {
		FileUploader.flumeServer = flumeServer;
	}

	public static String getClientId() {
		return clientId;
	}

	public static void setClientId(String clientId) {
		FileUploader.clientId = clientId;
	}

	public FileUploader(String threadName) {
		this.threadName = threadName;
	}

	public void setFilePath(String filePathes) {
		String[] dirs = filePathes.split(",");
		for (String dir : dirs) {
			try {
				DirInfo dirInfo = new DirInfo(dir, 0);
				dirInfos.add(dirInfo);
			} catch (Exception e) {
				logger.error("dir is not exist or not a directory, skip it. dir:" + dir);
			}
		}
	}

	private void loadFileConfig () throws ConfigurationException {
    	Configurations configurations = new Configurations();
        Configuration configuration = configurations.properties(confName);
        Iterator<String> keys = configuration.getKeys();
        while (keys.hasNext()) {
        	String key = keys.next();
        	switch (key) {
			case PIC_DIRECTORY_NAME:
				setFilePath(configuration.getString(key));
				break;
			case FLUME_SERVER_URL :
				flumeServer = configuration.getString(key);
				if (flumeServer != null && !flumeServer.contains("://")) {
		        	flumeServer = "http://" + flumeServer;
		        }
				break;
			case CLIENT_ID:
				setClientId(configuration.getString(key));
				break;
			default:
				break;
			}
        }
    }

	@Override
	protected void beforeTask() {
//		load file dirs from local config files
		try {
			loadFileConfig();
		} catch (ConfigurationException e){
			BeaverUtils.PrintStackTrace(e);
			logger.error("load config file error, msg:" + e.getMessage());
		}

//		load file dirs from remote server
		while (KEEP_RUNNING) {
			try {
				getFileTask();
				break;
			} catch (ConfigurationException | IOException e) {
				BeaverUtils.PrintStackTrace(e);
				logger.error("update task list error, msg:" + e.getMessage());
				BeaverUtils.sleep(10 * 1000);
			}
		}
	}

	@Override
	protected void doTask(Object taskObject) {
		DirInfo dirInfo = (DirInfo)taskObject;
		dirInfo.listSortUploadFiles();
	}

	@Override
	protected int getThreadNum() {
		return dirInfos.size();
	}

	@Override
	protected Object getTaskObject(int index) {
		return dirInfos.get(index);
	}

	@Override
	protected long getSleepTimeBetweenTaskInnerLoop() {
		return 200;
	}

	@Override
	protected String getTaskDescription() {
		return "file_uploader";
	}

	private void getFileTask() throws ConfigurationException, IOException {
		Configurations configurations = new Configurations();
		Configuration conf = configurations.properties(confName);
		String json = BeaverUtils.doGet(conf.getString("tasks-server.url") + clientId);
		ObjectMapper objectMapper = new ObjectMapper();
		JsonNode root = objectMapper.readTree(json);
		JsonNode dbs = root.get("databases");
		if (dbs == null) {
			logger.error("no databases entry, tasks:" + root.toString());
			return;
		}

		List<DirInfo> remoteSetDirs = new ArrayList<DirInfo>();
		for (int i = 0; i < dbs.size(); i++) {
			JsonNode db = dbs.get(i);
			if (db == null || !db.has("db")){
				logger.error("this task has no db entry, task:" + db);
				continue;
			}else if (db.get("db").asText().equals("DocumentFiles")) {
				JsonNode tables = db.get("tables");
				for (int j = 0; j < tables.size(); j++) {
					JsonNode table = tables.get(j);
					if (table != null && table.has("table") && table.has("xgsj")) {
						DirInfo dirInfo;
						try {
							dirInfo = new DirInfo(table.get("table").asText(), table.get("xgsj").asLong());
							remoteSetDirs.add(dirInfo);
							logger.info("get one dir from remote server, dir:" + table.toString());
						} catch (Exception e) {
							BeaverUtils.PrintStackTrace(e);
							logger.error("dir wrong, dir:" + table.toString());
						}
					}else {
						logger.error("find DocumentFiles db, but can't find table or xgsj. table:" + table);
					}
				}
			}else {
//					do nothing for now
			}
		}

		if (remoteSetDirs.size() > 0) {
			if (USE_REMOTE_DIRS) {
				dirInfos = remoteSetDirs;
			}
		}
	}

	public static void main(String[] args) {
		Thread fileUploaderThread = new Thread(new FileUploader("FileUploader"));
		fileUploaderThread.start();

		try {
			fileUploaderThread.join();
		} catch (InterruptedException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("join failed");
		}
	}
}