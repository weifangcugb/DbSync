package com.cloudbeaver.client.dbUploader;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.log4j.*;

import com.cloudbeaver.client.common.BeaverFatalException;
import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.client.common.SqlHelper;
import com.cloudbeaver.client.common.CommonUploader;
import com.cloudbeaver.client.dbbean.DatabaseBean;
import com.cloudbeaver.client.dbbean.MultiDatabaseBean;
import com.cloudbeaver.client.dbbean.TableBean;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;
import java.util.Map;

public class DbUploader extends CommonUploader{
    private static Logger logger = Logger.getLogger(DbUploader.class);

    private Map<String, String> conf;
    private String taskJson;
    private MultiDatabaseBean dbBeans;

	private static final String DB_TYPE_SQL = "sqldb";
	private static final String DB_TYPE_URL = "urldb";

    public String getTaskJson() {
		return taskJson;
	}

	public void setTaskJson(String taskJson) {
		this.taskJson = taskJson;
	}

	@Override
	public void setup() throws BeaverFatalException {
        try {
			conf = BeaverUtils.loadConfig(CONF_DBSYNC_DB_FILENAME);
	        if (conf.containsKey(CONF_CLIENT_ID) && conf.get(CONF_CLIENT_ID).contains("_")) {
				setClientId(conf.get(CONF_CLIENT_ID));
				setPrisonIdByClientId(conf.get(CONF_CLIENT_ID));
			}else {
				logger.fatal("no client.id or client.id has no '_', in config file");
				throw new BeaverFatalException("no client.id or client.id has no '_' in config file");
			}
		} catch (IOException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.fatal("load config failed, please restart process. confName:" + CONF_DBSYNC_DB_FILENAME + " msg:" + e.getMessage());
			throw new BeaverFatalException("load config failed, please restart process. confName:" + CONF_DBSYNC_DB_FILENAME + " msg:" + e.getMessage(), e);
		}

//      get tasks from web server
        while (true) {
//			for this version, only load tasks once
    		try {
				loadTasks();
				break;
			} catch (IOException e) {
				BeaverUtils.PrintStackTrace(e);
				logger.error("get tasks failed, url:" + conf.get(CONF_TASK_SERVER_URL) + " msg:" + e.getMessage() + " json:" + getTaskJson());
				BeaverUtils.sleep(60 * 1000);
			}
        }
	}

	@Override
	public int getThreadNum() {
		return dbBeans.getDatabases().size();
	}

	@Override
	public Object getTaskObject(int threadIndex) {
		DatabaseBean dbBean = dbBeans.getDatabases().get(threadIndex);
		if (shouldSkipDb(dbBean)) {
			return null;
		}

		return dbBean;
	}

	public boolean shouldSkipDb(DatabaseBean dbBean) {
		return dbBean.getDb().equals(TASK_FILEDB_NAME);
	}

	@Override
	public void doTask(Object taskObject) throws BeaverFatalException{
		DatabaseBean dbBean = (DatabaseBean)taskObject;
		dbBean.setQueryTime((new Date()).toString());

        for (TableBean tableBean : dbBean.getTables()) {
//        	keep trying until get all data from this table
        	while (true) {
        		Date date = new Date();
        		dbBean.setQueryTime(date.toString());
        		tableBean.setQueryTime(date.toString());

        		String dbData = null;
        		String maxXgsj = null;
        		if (dbBean.getType().equals(DB_TYPE_SQL)) {
        	        logger.debug("Executing query : " + tableBean.getSqlString(prisonId, dbBean.getDb(), dbBean.getRowversion(), DB_QEURY_LIMIT));

        	        JSONArray jArray = new JSONArray();
        			try {
        				maxXgsj = SqlHelper.execSqlQuery(prisonId, dbBean, tableBean, this, DB_QEURY_LIMIT, jArray);
        			} catch (SQLException e) {
        				BeaverUtils.PrintStackTrace(e);
        				logger.error("sql query faild, msg:" + e.getMessage() + " url:" + conf.get("db." + dbBean.getDb() + ".url"));

        				SqlHelper.removeConnection(dbBean);
        				BeaverUtils.sleep(1000);
        				continue;
        			}

        	        logger.debug("get db data, json:" + jArray.toString());
					if (jArray.isEmpty()) {
//						start to query next table
						break;
					}

					dbData = jArray.toString();
				}else if (dbBean.getType().equals(DB_TYPE_URL)) {
					
				}else {
					throw new BeaverFatalException("db type is wrong, type can only be 'sqldb' or 'urldb'");
				}

                String flumeJson = null;
				try {
					flumeJson = BeaverUtils.compressAndFormatFlumeHttp(dbData);
				} catch (IOException e) {
//					this is impossible unless system memory has some error, as I think
					BeaverUtils.PrintStackTrace(e);
					logger.error("write gzip stream to memory error, msg:" + e.getMessage());
					BeaverUtils.sleep(1000);
					continue;
				}

                logger.debug("upload db data, data:" + flumeJson);

                try {
    				BeaverUtils.doPost(conf.get(CONF_FLUME_SERVER_URL), flumeJson);
    				if (maxXgsj != null) {
    					tableBean.setXgsj(maxXgsj);
					}
//    				logger.info("send db data to flume server, json:" + flumeJson);
    			} catch (IOException e) {
    				BeaverUtils.PrintStackTrace(e);
    				logger.error("post json to flume server failed, server:" + conf.get(CONF_FLUME_SERVER_URL) + " json:" + flumeJson);
    				BeaverUtils.sleep(1000);
    			}

                BeaverUtils.sleep(100);
			}
        	BeaverUtils.sleep(100);
        }
	}

	@Override
	protected void doHeartBeat() {
		JSONArray dbsReport = new JSONArray();
		for (DatabaseBean dbBean : dbBeans.getDatabases()) {
			if (shouldSkipDb(dbBean)) {
				continue;
			}

			JSONObject db= new JSONObject();
			db.put("hdfs_client", clientId);
			db.put("hdfs_prison", prisonId);
			db.put("hdfs_db", dbBean.getDb());
			db.put(REPORT_TYPE, REPORT_TYPE_HEARTBEAT);
			db.put("queryTime", dbBean.getQueryTime());
			JSONArray tables = new JSONArray();
			for (TableBean tBean : dbBean.getTables()) {
				JSONObject table= new JSONObject();
				table.put("table", tBean.getTable());
				table.put("xgsj", tBean.getXgsj());
				table.put("queryTime", tBean.getQueryTime());
				tables.add(table);
			}
			db.put("tables", tables);
			dbsReport.add(db);
		}

		try {
			BeaverUtils.doPost(conf.get(CONF_FLUME_SERVER_URL), BeaverUtils.compressAndFormatFlumeHttp(dbsReport.toString()));
		} catch (IOException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("send heart beat error. msg:" + e.getMessage());
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

	public static void startDbUploader(){
		DbUploader dbUploader = new DbUploader();
    	Thread dbUploaderThread = new Thread(dbUploader);
    	dbUploaderThread.start();

    	try {
    		dbUploaderThread.join();
		} catch (InterruptedException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("dbuploader join failed, msg:" + e.getMessage());
		}
	}

	private void loadTasks() throws IOException, BeaverFatalException {
        String json = BeaverUtils.doGet(conf.get(CONF_TASK_SERVER_URL) + clientId);
        logger.debug("fetch tasks, tasks:" + json);

        setTaskJson(json);

        ObjectMapper objectMapper = new ObjectMapper();
        dbBeans = objectMapper.readValue(taskJson, MultiDatabaseBean.class);
        for (DatabaseBean dbBean : dbBeans.getDatabases()) {
        	dbBean.setDbUrl(conf.get("db." + dbBean.getDb() + ".url"));
        	dbBean.setDbUserName(conf.get("db." + dbBean.getDb() + ".username"));
        	dbBean.setDbPassword(conf.get("db." + dbBean.getDb() + ".password"));
        	String dbType = conf.get("db." + dbBean.getDb() + ".type");
        	if (dbType.equals(DB_TYPE_SQL) || dbType.equals(DB_TYPE_URL)) {
        		dbBean.setType(dbType);
			}else {
				throw new BeaverFatalException("dbtype set error, only 'urldb' or 'sqldb'");
			}
        }
    }

    public static void main(String[] args) {
    	startDbUploader();
    }
}
