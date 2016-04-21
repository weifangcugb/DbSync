package com.cloudbeaver.client.dbUploader;

import org.apache.log4j.*;

import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.client.common.FixedNumThreadPool;
import com.cloudbeaver.client.dbbean.DatabaseBean;
import com.cloudbeaver.client.dbbean.MultiDatabaseBean;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

public class DbUploader extends FixedNumThreadPool{
	private final static String CONF_CLIENT_ID = "client.id";
	private final static String CONFIG_FILE_NAME = "SyncClient.properties";
    private final static String TASK_SERVER_URL = "tasks-server.url";
    private static String FILE_UPLOAD_DB_NAME = "DocumentFiles";

    private static Logger logger = Logger.getLogger(DbUploader.class);

    private Map<String, String> conf = null;
    private String taskJson = null;
    private MultiDatabaseBean multiDatabaseBean = null;

    private String clientId = null;

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getTaskJson() {
		return taskJson;
	}

	public void setTaskJson(String taskJson) {
		this.taskJson = taskJson;
	}

	@Override
	public void beforeTask() {
        try {
			conf = BeaverUtils.loadConfig(CONFIG_FILE_NAME);
	        if (conf.containsKey(CONF_CLIENT_ID)) {
				setClientId(conf.get(CONF_CLIENT_ID));
			}else {
				logger.fatal("no client.id in config file");
				return;
			}
		} catch (IOException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.fatal("load config failed, please restart process. confName:" + CONFIG_FILE_NAME + " msg:" + e.getMessage());
			return;
		}

//      get tasks from web server
        while (true) {
//			for this version, only load tasks once
    		try {
				loadTasks();
				break;
			} catch (IOException e) {
				BeaverUtils.PrintStackTrace(e);
				logger.error("get tasks failed, url:" + TASK_SERVER_URL + " msg:" + e.getMessage() + " json:" + getTaskJson());
				BeaverUtils.sleep(60 * 1000);
			}
        }
	}

	@Override
	public int getThreadNum() {
		return multiDatabaseBean.getDatabases().size();
	}


	@Override
	public Object getTaskObject(int threadIndex) {
		DatabaseBean dbBean = multiDatabaseBean.getDatabases().get(threadIndex);
		if (shouldSkipDb(dbBean)) {
			return null;
		}

		return dbBean;
	}

	public boolean shouldSkipDb(DatabaseBean dbBean) {
		return dbBean.getDb().equals(FILE_UPLOAD_DB_NAME);
	}

	@Override
	public void doTask(Object taskObject) {
		DatabaseBean dbBean = (DatabaseBean)taskObject;
        String flumeJson = getDbUploadData(dbBean);
        try {
			BeaverUtils.doPost(conf.get("flume-server.url"), flumeJson);
		} catch (IOException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("post faild, msg:" + e.getMessage() + " url:" + conf.get("flume-server.url"));
		}
	}

	public String getDbUploadData(DatabaseBean dbBean) {
		String dbInfo = dbBean.query();
		String oriDbInfo = dbInfo;

        dbInfo = dbInfo.replaceAll("\"", "\\\\\"");
        String flumeJson = "[{ \"headers\" : {}, \"body\" : \"[" + dbInfo + "]\" }]";
        logger.debug("upload db data, data:" + flumeJson);

        return oriDbInfo;
	}

	@Override
	protected long getSleepTimeBetweenTaskInnerLoop() {
		return 3 * 1000;
	}

	@Override
	protected String getTaskDescription() {
		return "upload_db_data";
	}

	public static void startDbUploader(){
    	Thread dbUploader = new Thread(new DbUploader());
    	dbUploader.start();

    	try {
			dbUploader.join();
		} catch (InterruptedException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("dbuploader join failed, msg:" + e.getMessage());
		}
	}

    private void loadTasks() throws IOException {
        String json = BeaverUtils.doGet(conf.get(TASK_SERVER_URL) + clientId);
        logger.debug("fetch tasks, tasks:" + json);

        setTaskJson(json);

        ObjectMapper objectMapper = new ObjectMapper();
        multiDatabaseBean = objectMapper.readValue(taskJson, MultiDatabaseBean.class);
        multiDatabaseBean.setConf(conf);
    }

    public static void main(String[] args) {
    	startDbUploader();
    }
}