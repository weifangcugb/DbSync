package com.cloudbeaver.client.common;

public abstract class CommonUploader extends FixedNumThreadPool{
//	private final static String CONF_FILE_PREFIX = "/opt/dbsync/";
	private static final String CONF_FILE_PREFIX = "conf/";
	public static final String CONF_DBSYNC_DB_FILENAME = CONF_FILE_PREFIX + "SyncClient_DB.properties";
	public static final String CONF_DBSYNC_FILE_FILENAME = CONF_FILE_PREFIX + "SyncClient_File.properties";
	public static final String CONF_KAFKA_CONSUMER_FILE_NAME = CONF_FILE_PREFIX + "SyncConsumer.properties";

	public static final int DB_QEURY_LIMIT_DB = 50;
	public static final int DB_QEURY_LIMIT_WEB_SERVICE = 30;

	public static final String CONF_CLIENT_ID = "client.id";
	public static final String CONF_FLUME_SERVER_URL = "flume-server.url";
    public static final String CONF_TASK_SERVER_URL = "tasks-server.url";
	public static final String TASK_DB_NAME = "DocumentDB";
	public static final String TASK_FILEDB_NAME = "DocumentFiles";

	public static final String REPORT_TYPE= "dataType";
	public static final String REPORT_TYPE_HEARTBEAT = "HeartBeat";

	public static final String CONF_PIC_DIRECTORY_NAME = "db.DocumentFiles.url";
	public static final String CONF_DIR_DATA_FORMAT = "yyyy-MM-dd-hh-mm-ss";

	public static final String DB_TYPE_SQL_SERVER = "sqlserver";
	public static final String DB_TYPE_SQL_ORACLE = "oracle";
	public static final String DB_TYPE_SQL_SQLITE = "sqlite";
	public static final String DB_TYPE_WEB_SERVICE = "webservice";
	public static final String DB_EMPTY_ROW_VERSION = "0";

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
