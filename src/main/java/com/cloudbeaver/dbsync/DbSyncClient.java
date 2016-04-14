package com.cloudbeaver.dbsync;

import org.apache.log4j.*;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.configuration2.*;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by gaobin on 16-4-6.
 */
public class DbSyncClient {

    private final String DB_SYNC_CLIENT_CONFIG_FILE_NAME = "DbSyncClient.conf";
    private final String TASKS_SERVER_URL = "tasks-server.url";
    private static Logger logger = Logger.getLogger(DbSyncClient.class);

    private Map<String, String> conf;
    private Map<String, String> dbConf;
    private String taskJson;
    private Configurations configurations;
    private WatcherManager watcherManager;

    private String clientId;

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

    public void fetchTasks () {
        System.out.println(conf.get(TASKS_SERVER_URL) + getClientId());
        String json = HttpClientHelper.get(conf.get(TASKS_SERVER_URL) + clientId);
        logger.debug("Tasks : " + json);
        setTaskJson(json);
        reloadTasks();
    }

    private void reloadTasks () {
        if (taskJson == null || taskJson.length() == 0) {
            return;
        }
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            watcherManager = objectMapper.readValue(taskJson, WatcherManager.class);
            watcherManager.setConf(dbConf);
        } catch (IOException e) {
            e.printStackTrace();
            logger.fatal("Create bean watcherManager failed : " + e.getMessage());
            System.exit(1);
        }
    }

    private void loadConfig () {
        conf = new HashMap<String, String>();
        dbConf = new HashMap<String, String>();
        configurations = new Configurations();
        try {
            Configuration configuration
                    = configurations.properties(DB_SYNC_CLIENT_CONFIG_FILE_NAME);
            Iterator<String> keys = configuration.getKeys();
            while (keys.hasNext()) {
                String key = keys.next();
                if (! key.startsWith("db.")) {
                    conf.put(key, configuration.getString(key));
                } else {
                    dbConf.put(key, configuration.getString(key));
                }
            }
        } catch (ConfigurationException e) {
            e.printStackTrace();
            logger.error("Read conf from " + DB_SYNC_CLIENT_CONFIG_FILE_NAME + " failed : "
                    + e.getMessage());
        }
    }

    public DbSyncClient () {
        loadConfig();
        if (conf.containsKey("client.id")) {
            setClientId(conf.get("client.id"));
        } else {
            setClientId("1");
        }
    }

    public String query() {
        //assert (watcherManager != null);
        if (watcherManager == null)
            return "";
        return watcherManager.query();
    }

    public void sendToFlume (String str) {
        str = str.replaceAll("\"", "\\\\\"");
        String flumeJson = "[{ \"headers\" : {}, \"body\" : \"" + str + "\" }]";
        HttpClientHelper.post(conf.get("flume-server.url"), flumeJson);
    }

    public void queryAndSendToFlume () {
        sendToFlume(query());
    }

    protected void printConf() {
        // DEBUG
        for (String key : conf.keySet()) {
            System.out.println(key + " : " + conf.get(key));
        }
        for (String key : dbConf.keySet()) {
            System.out.println(key + " : " + dbConf.get(key));
        }
    }

    public static void main(String[] args) {

        DbSyncClient dbSyncClient = new DbSyncClient();
        dbSyncClient.fetchTasks();

        while (true) {
            dbSyncClient.queryAndSendToFlume();
            try {
                // Thread.sleep(1000 * 3); // DEBUG QUICKLY
                Thread.sleep(1000 * 60 * 3); // PRODUCT
            } catch (InterruptedException e) {
                e.printStackTrace();
                logger.debug("Sleep Interrupted !");
                break;
            }
        }

        // 下面都是测试用的。
        dbSyncClient.printConf();

        String brokerList = HttpClientHelper.get("http://br0:8088/bls");
        System.out.println(brokerList);

        // 405
        String brokerListPost = HttpClientHelper.post("http://bing.com/");
        System.out.println(brokerListPost);
        return;
    }

}
