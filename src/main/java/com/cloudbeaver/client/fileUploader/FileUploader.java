package com.cloudbeaver.client.fileUploader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.client.common.FixedNumThreadPool;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FileUploader extends FixedNumThreadPool {
    private static Logger logger = Logger.getLogger(FileUploader.class);

    public final static String CONF_FILE_NAME = "SyncClient.properties";
    public final static String CONF_TASK_SERVER = "tasks-server.url";
	public static final String PIC_DIRECTORY_NAME = "db.DocumentFiles.url";
	public static final String FLUME_SERVER_URL = "flume-server.url";
	public static final String CONF_CLIENT_ID = "clientid";

    public final static String FILE_UPLOAD_DB_NAME = "DocumentFiles";

	private static final boolean USE_REMOTE_DIRS = false;

	private static String taskServer = null;
	private static String flumeServer = null;
	private static String clientId = null;

	private Map<String, String> conf = null;

	private List<DirInfo> dirInfos = new ArrayList<DirInfo>();
	private String threadName = null;

	public static String getTaskServer() {
		return flumeServer;
	}

	public static void setTaskServer(String taskServer) {
		FileUploader.taskServer = taskServer;
	}
	
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

	private void loadFileConfig () throws IOException {
		conf = BeaverUtils.loadConfig(CONF_FILE_NAME);
		Set<String> keys = conf.keySet();
		for (String key : keys) {
        	switch (key) {
			case PIC_DIRECTORY_NAME:
				setFilePath(conf.get(key));
				break;
			case FLUME_SERVER_URL :
				flumeServer = conf.get(key);
				if (flumeServer != null && !flumeServer.contains("://")) {
		        	flumeServer = "http://" + flumeServer;
		        }
				break;
			case CONF_CLIENT_ID:
				setClientId(conf.get(key));
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
			if (clientId == null || taskServer == null || flumeServer == null) {
				logger.fatal("no client.id in conf file, confName:" + CONF_FILE_NAME);
				return;
			}
		} catch (IOException e){
			BeaverUtils.PrintStackTrace(e);
			logger.error("load config file error, msg:" + e.getMessage());
			return;
		}

//		load file dirs from remote server
		while (isRunning()) {
			try {
				getFileTask();
				break;
			} catch (IOException e) {
				BeaverUtils.PrintStackTrace(e);
				logger.error("update task list error, msg:" + e.getMessage());
				BeaverUtils.sleep(10 * 1000);
			}
		}
	}

	@Override
	protected void doTask(Object taskObject) {
		DirInfo dirInfo = (DirInfo)taskObject;
		dirInfo.listAndSortFiles();
		dirInfo.uploadFiles();
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

	private void getFileTask() throws  IOException {
		String json = BeaverUtils.doGet(taskServer + clientId);
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
//				do nothing for now
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