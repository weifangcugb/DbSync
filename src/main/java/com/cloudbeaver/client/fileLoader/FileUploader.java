package com.cloudbeaver.client.fileLoader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.log4j.Logger;

import com.cloudbeaver.client.common.BeaverUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import sun.misc.Signal;
import sun.misc.SignalHandler;

public class FileUploader implements Runnable {
	private static Logger logger = Logger.getLogger(FileUploader.class);

	private static final String PIC_DIRECTORY_NAME = "db.DocumentFiles.url";
	private static final String FLUME_SERVER_URL = "flume-server.url";
	private static final String CLIENT_ID = "clientid";
	private static final boolean USE_REMOTE_DIRS = false;

	private static boolean KEEP_RUNNING = true;

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
	public void run() {
		logger.info("start to upload files, threadName:" + threadName);

//		load file dirs from local config files
		try {
			loadFileConfig();
		} catch (ConfigurationException e){
			BeaverUtils.PrintStackTrace(e);
			logger.error("load config file error, msg:" + e.getMessage());
		}

//		load file dirs from remote server
		try {
			getFileTask();
		} catch (ConfigurationException | IOException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("update task list error, msg:" + e.getMessage());
		}

		ExecutorService executor = Executors.newCachedThreadPool();
		for (final DirInfo dirInfo : dirInfos) {
			logger.debug("handle dirinfo, path:" + dirInfo.dir.getAbsolutePath());
			executor.submit(new Runnable() {
				@Override
				public void run() {
					logger.info("start thread to upload files, threadId:" + Thread.currentThread().getId());
					while(KEEP_RUNNING){
						dirInfo.listSortUploadFiles();
						try {
//							have a break after each time
							Thread.sleep(5000);
						} catch (InterruptedException e) {
							BeaverUtils.PrintStackTrace(e);
							logger.error("sleep interrupted, msg:" + e.getMessage());
						}
					}
					logger.info("exit thread to upload files, threadId:" + Thread.currentThread().getId());
				}
			});
		}

		while (KEEP_RUNNING) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				BeaverUtils.PrintStackTrace(e);
				logger.error("sleep interrupted, msg:" + e.getMessage());
			}
		}

		executor.shutdown();

		while ( !executor.isTerminated() ) {
			try {
				executor.awaitTermination(1, TimeUnit.SECONDS);
			} catch (InterruptedException e){
				logger.error("sleep interrupted, msg:" + e.getMessage());
			}
		}
	}

	private void getFileTask() throws ConfigurationException, IOException {
		Configurations configurations = new Configurations();
		Configuration conf = configurations.properties(confName);
			String json = BeaverUtils.doGet(conf.getString("tasks-server.url") + clientId);//HttpClientHelper.get(conf.getString("tasks-server.url") + clientId);
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

		Signal sig = new Signal("USR2");
		Signal.handle(sig, new StopHandler());

		try {
			fileUploaderThread.join();
		} catch (InterruptedException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("join failed");
		}
	}

	public static void stopRunning() {
		KEEP_RUNNING = false;
	}
}

class StopHandler implements SignalHandler{
	Logger logger = Logger.getLogger(StopHandler.class);

	@Override
	public void handle(Signal sig) {
		logger.info("get signal, signal:" + sig);
		FileUploader.stopRunning();
	}
	
}