package com.cloudbeaver.client.common;

import java.util.HashMap;
import java.util.Map;

public abstract class CommonUploader extends FixedNumThreadPool{
//	private final static String CONF_FILE_PREFIX = "/opt/dbsync/";
	private static final String CONF_FILE_PREFIX = "conf/";
	public static final String CONF_DBSYNC_DB_FILENAME = CONF_FILE_PREFIX + "SyncClient_DB.properties";
	public static final String CONF_DBSYNC_FILE_FILENAME = CONF_FILE_PREFIX + "SyncClient_File.properties";
	public static final String CONF_KAFKA_CONSUMER_FILE_NAME = CONF_FILE_PREFIX + "SyncConsumer.properties";
	public static final int DB_QEURY_LIMIT = 30;

	public static final String CONF_CLIENT_ID = "client.id";
	public static final String CONF_FLUME_SERVER_URL = "flume-server.url";
    public static final String CONF_TASK_SERVER_URL = "tasks-server.url";
	public static final String TASK_DB_NAME = "DocumentDB";
	public static final String TASK_FILEDB_NAME = "DocumentFiles";

	public static final String REPORT_TYPE= "dataType";
	public static final String REPORT_TYPE_HEARTBEAT = "HeartBeat";

	public static final String CONF_PIC_DIRECTORY_NAME = "db.DocumentFiles.url";
	public static final String CONF_DIR_DATA_FORMAT = "yyyy-MM-dd-hh-mm-ss";

    protected static String clientId;
	protected static String prisonId;

	public static String getPrisonId() {
		return prisonId;
	}

	protected static void setPrisonIdByClientId(String clientId) {
		prisonId = clientId.split("_")[0];
	}

	public static String getClientId() {
        return clientId;
    }

    public static void setClientId(String clientId2) {
        clientId = clientId2;
    }
}
