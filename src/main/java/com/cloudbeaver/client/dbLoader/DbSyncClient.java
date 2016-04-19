package com.cloudbeaver.client.dbLoader;

import org.apache.log4j.*;

import com.cloudbeaver.client.bean.MultiDatabaseBean;
import com.cloudbeaver.client.common.BeaverUtils;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.configuration2.*;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DbSyncClient {
    private final static String CONFIG_FILE_NAME = "DbSyncClient.conf";
    private final static String TASK_SERVER_URL = "tasks-server.url";
    private static Logger logger = Logger.getLogger(DbSyncClient.class);

    private Map<String, String> conf = null;
    private Map<String, String> dbConf = null;
    private String taskJson = null;
    private Configurations configurations = null;
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

    public void getTasks () throws IOException {
//        String json = HttpClientHelper.get(conf.get(TASKS_SERVER_URL) + clientId);
        String json = BeaverUtils.doGet(conf.get(TASK_SERVER_URL) + clientId);
        logger.debug("fetch tasks, tasks:" + json);

        setTaskJson(json);

        ObjectMapper objectMapper = new ObjectMapper();
        multiDatabaseBean = objectMapper.readValue(taskJson, MultiDatabaseBean.class);
        multiDatabaseBean.setConf(dbConf);
    }

    public void loadConfig () throws ConfigurationException {
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

    public String query() {
        if (multiDatabaseBean == null)
            return "";
        return multiDatabaseBean.query();
    }

    public void sendToFlume (String str) throws IOException {
        str = str.replaceAll("\"", "\\\\\"");
        String flumeJson = "[{ \"headers\" : {}, \"body\" : \"" + str + "\" }]";
//        HttpClientHelper.post(conf.get("flume-server.url"), flumeJson);
        BeaverUtils.doPost(conf.get("flume-server.url"), flumeJson);
    }

    public void queryAndSendToFlume () throws IOException {
        sendToFlume(query());
    }

    public static void main(String[] args) {
        DbSyncClient dbSyncClient = new DbSyncClient();
        try {
			dbSyncClient.loadConfig();
		} catch (ConfigurationException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.fatal("load config failed, please restart process. confName:" + CONFIG_FILE_NAME + " msg:" + e.getMessage());
			return;
		}

        while (true) {
//			for this version, only load tasks once
        	if (dbSyncClient.getTaskJson() == null) {
        		try {
					dbSyncClient.getTasks();
				} catch (IOException e) {
					BeaverUtils.PrintStackTrace(e);
					logger.error("get tasks failed, url:" + TASK_SERVER_URL + " msg:" + e.getMessage() + " json:" + dbSyncClient.getTaskJson());
					BeaverUtils.sleep(60 * 1000);
					continue;
				}
			}

            try {
				dbSyncClient.queryAndSendToFlume();
			} catch (IOException e) {
				BeaverUtils.PrintStackTrace(e);
				logger.error("query and post db data error, msg:" + e.getMessage());
			}

            BeaverUtils.sleep(3 * 60 * 1000);
        }
    }
}
