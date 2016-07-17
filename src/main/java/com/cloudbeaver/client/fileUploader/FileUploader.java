package com.cloudbeaver.client.fileUploader;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

import com.cloudbeaver.client.common.BeaverFatalException;
import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.client.common.CommonUploader;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FileUploader extends CommonUploader {
	public static boolean USE_REMOTE_DIRS = true;
	public static int LARGE_PIC_SIZE_BARRIER = 600 * 1024;

    private static Logger logger = Logger.getLogger(FileUploader.class);

	private static String taskServer = null;
	private static String flumeServer = null;

	private Map<String, String> conf = null;

	private List<DirInfo> dirInfos = new ArrayList<DirInfo>();

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

	public void setFilePath(String filePathes) {
		String[] dirConfs = filePathes.split(",");
		for (String dirConf : dirConfs) {
			try {
				DirInfo dirInfo;
				int index = dirConf.indexOf('?');
				if (index != -1 && dirConf.length() > index + 1) {
					String dir = dirConf.substring(0, index);
					String time = dirConf.substring(index + 1);
					if (time.indexOf('-') != -1) {
						SimpleDateFormat sdf = new SimpleDateFormat(CONF_DIR_DATA_FORMAT);
						Date date = sdf.parse(time);
						dirInfo = new DirInfo(dir, date.getTime());
					}else{
						dirInfo = new DirInfo(dir, time);
					}
				}else {
					dirInfo = new DirInfo(dirConf, "" + 0);
				}

				dirInfos.add(dirInfo);
			} catch (ParseException | IOException e) {
				logger.error("dir is not exist or not a directory, skip it. dir:" + dirConf);
			}
		}
	}

	private void loadFileConfig () throws IOException, ParseException {
		conf = BeaverUtils.loadConfig(CONF_DBSYNC_FILE_FILENAME);
		Set<String> keys = conf.keySet();
		for (String key : keys) {
        	switch (key) {
			case CONF_PIC_DIRECTORY_NAME:
				if (!USE_REMOTE_DIRS) {
					setFilePath(conf.get(key));
				}
				break;
			case CONF_TASK_SERVER_URL:
				setTaskServer(conf.get(key));
				break;
			case CONF_FLUME_SERVER_URL:
				flumeServer = conf.get(key);
				if (flumeServer != null && !flumeServer.contains("://")) {
		        	flumeServer = "http://" + flumeServer;
		        }
				break;
			case CONF_CLIENT_ID:
				setClientId(conf.get(key));
				setPrisonIdByClientId(conf.get(key));
				break;
			default:
				break;
			}
		}
    }

	@Override
	protected void setup() throws BeaverFatalException {
		dirInfos.clear();
		if(conf != null){
			conf.clear();
		}

//		load file dirs from local config files
		try {
			loadFileConfig();
			if (clientId == null || prisonId == null || taskServer == null || flumeServer == null) {
				logger.fatal("no client.id in conf file, confName:" + CommonUploader.CONF_DBSYNC_FILE_FILENAME);
				throw new BeaverFatalException("no client.id in config file");
			}
		} catch (IOException | ParseException e){
			BeaverUtils.PrintStackTrace(e);
			logger.error("load config file error, msg:" + e.getMessage());
			throw new BeaverFatalException("load config failed, please restart process. confName:" + CommonUploader.CONF_DBSYNC_FILE_FILENAME + " msg:" + e.getMessage(), e);
		}

//		load file dirs from remote server
		while (isRunning() && USE_REMOTE_DIRS) {
			try {
				getFileTask();
				break;
			} catch (IOException e) {
				BeaverUtils.PrintStackTrace(e);
				logger.error("get task error, server: " + taskServer + " msg:" + e.getMessage());
				BeaverUtils.sleep(10 * 1000);
			}
		}
	}

	@Override
	protected void doTask(Object taskObject) {
		DirInfo dirInfo = (DirInfo)taskObject;
		dirInfo.setQueryTime((new Date()).toString());
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

	private void getFileTask() throws IOException {
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
			}else if (db.get("db").asText().equals(TASK_FILEDB_NAME)) {
				JsonNode tables = db.get("tables");
				for (int j = 0; j < tables.size(); j++) {
					JsonNode table = tables.get(j);
					if (table != null && table.has("table") && table.has("xgsj")) {
						DirInfo dirInfo;
						try {
							dirInfo = new DirInfo(table.get("table").asText(), "" + BeaverUtils.hexTolong(table.get("xgsj").asText()));
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
//				other dbs, do nothing
			}
		}

		if (USE_REMOTE_DIRS) {
        	if (remoteSetDirs.size() > 0) {
        		dirInfos = remoteSetDirs;
        	}else {
        		logger.fatal("all dirs got from server are not exist, task:" + json);
        		throw new IOException("dir are all not exist, will try again");
        	}
		}
	}

	@Override
	protected void doHeartBeat() {
		JSONArray dbsReport = new JSONArray();
		JSONObject fileDb = new JSONObject();
		fileDb.put("hdfs_client", clientId);
		fileDb.put("hdfs_prison", prisonId);
		fileDb.put("hdfs_db", TASK_FILEDB_NAME);
		fileDb.put(REPORT_TYPE, REPORT_TYPE_HEARTBEAT);
		JSONArray tables = new JSONArray();
		for (DirInfo dirInfo: dirInfos) {
			JSONObject table = new JSONObject();
			table.put("table", dirInfo.getDirName());
			table.put("xgsj", "" + dirInfo.getMiniChangeTimeAsHexString());
			table.put("queryTime", dirInfo.getQueryTime());
			table.put("uploadingFile", dirInfo.getUploadingFile());
			tables.add(table);
		}
		fileDb.put("tables", tables);
		dbsReport.add(fileDb);

		try {
			BeaverUtils.doPost(conf.get(CONF_FLUME_SERVER_URL), BeaverUtils.compressAndFormatFlumeHttp(dbsReport.toString()));
		} catch (IOException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("send heart beat error. msg:" + e.getMessage());
		}
	}

	public static void startFileUploader(){
		logger.info("starting fileUploader");

		Thread fileUploaderThread = new Thread(new FileUploader());
		fileUploaderThread.start();

		try {
			fileUploaderThread.join();
		} catch (InterruptedException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("join failed");
		}
	}

	public static void main(String[] args) {
		startFileUploader();
	}
}
