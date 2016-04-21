package com.cloudbeaver.client.dbUploader;

import org.apache.log4j.*;

import com.cloudbeaver.client.bean.DatabaseBean;
import com.cloudbeaver.client.bean.MultiDatabaseBean;
import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.client.common.FixedNumThreadPool;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.configuration2.*;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DbUploader extends FixedNumThreadPool{
    private final static String CONFIG_FILE_NAME = "DbSyncClient.conf";
    private final static String TASK_SERVER_URL = "tasks-server.url";
    private static Logger logger = Logger.getLogger(DbUploader.class);

    private Map<String, String> conf = null;
    private Map<String, String> dbConf = null;
    private String taskJson = null;
    private Configurations configurations = null;
    private MultiDatabaseBean multiDatabaseBean = null;

    private String clientId = null;

    private DbUploader dbUploader = null;

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

    public void loadTasks() throws IOException {
        String json = BeaverUtils.doGet(conf.get(TASK_SERVER_URL) + clientId);
        logger.debug("fetch tasks, tasks:" + json);

        setTaskJson(json);

        ObjectMapper objectMapper = new ObjectMapper();
        multiDatabaseBean = objectMapper.readValue(taskJson, MultiDatabaseBean.class);
        multiDatabaseBean.setConf(dbConf);
    }

    public void loadConfig() throws ConfigurationException {
        conf = new HashMap<String, String>();
        dbConf = new HashMap<String, String>();
        configurations = new Configurations();

        Configuration configuration
                = configurations.properties(CONFIG_FILE_NAME);
        Iterator<String> keys = configuration.getKeys();
        while (keys.hasNext()) {
            String key = keys.next();
            if (! key.startsWith("db.")) {
                conf.put(key, configuration.getString(key));
            } else {
                dbConf.put(key, configuration.getString(key));
            }
        }

        if (conf.containsKey("client.id")) {
			setClientId(conf.get("client.id"));
		}else {
			throw new ConfigurationException("no config key, client.id");
		}
    }

/*
 *     this function should be just used for unit-test
 *     the returned string is a json contained db content
 */
    public String doQuerySingleThread() throws IOException {
        if (multiDatabaseBean == null)
            return "";
        String dbInfo = multiDatabaseBean.query();
        dbInfo = dbInfo.replaceAll("\"", "\\\\\"");
        String flumeJson = "[{ \"headers\" : {}, \"body\" : \"" + dbInfo + "\" }]";
        BeaverUtils.doPost(conf.get("flume-server.url"), flumeJson);
        return flumeJson;
    }

	@Override
	public void beforeTask() {
        dbUploader = new DbUploader();
        try {
			dbUploader.loadConfig();
		} catch (ConfigurationException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.fatal("load config failed, please restart process. confName:" + CONFIG_FILE_NAME + " msg:" + e.getMessage());
			return;
		}

//      get tasks from web server
        while (true) {
//			for this version, only load tasks once
    		try {
				dbUploader.loadTasks();
				break;
			} catch (IOException e) {
				BeaverUtils.PrintStackTrace(e);
				logger.error("get tasks failed, url:" + TASK_SERVER_URL + " msg:" + e.getMessage() + " json:" + dbUploader.getTaskJson());
				BeaverUtils.sleep(60 * 1000);
			}
        }
	}

	@Override
	public int getThreadNum() {
		return dbUploader.multiDatabaseBean.getDatabases().size();
	}


	@Override
	protected Object getTaskObject(int threadIndex) {
		return dbUploader.multiDatabaseBean.getDatabases().get(threadIndex);
	}

	@Override
	protected void doTask(Object taskObject) {
		DatabaseBean dbBean = (DatabaseBean)taskObject;
		String dbInfo = dbBean.query();

        dbInfo = dbInfo.replaceAll("\"", "\\\\\"");
        String flumeJson = "[{ \"headers\" : {}, \"body\" : \"[" + dbInfo + "]\" }]";
        logger.debug("upload db data, data:" + flumeJson);

        try {
			BeaverUtils.doPost(conf.get("flume-server.url"), flumeJson);
		} catch (IOException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("post faild, msg:" + e.getMessage() + " url:" + conf.get("flume-server.url"));
		}
	}

	@Override
	protected long getSleepTimeBetweenTaskInnerLoop() {
		return 3 * 1000;
	}

	@Override
	protected String getTaskDescription() {
		return "upload_db_data";
	}
	
    public static void main(String[] args) {
    	Thread dbUploader = new Thread(new DbUploader());
    	dbUploader.start();

    	try {
			dbUploader.join();
		} catch (InterruptedException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("dbuploader join failed, msg:" + e.getMessage());
		}
    }
}