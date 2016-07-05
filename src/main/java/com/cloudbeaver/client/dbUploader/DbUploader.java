package com.cloudbeaver.client.dbUploader;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.log4j.*;

import com.cloudbeaver.client.common.BeaverFatalException;
import com.cloudbeaver.client.common.BeaverTableIsFullException;
import com.cloudbeaver.client.common.BeaverTableNeedRetryException;
import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.client.common.SqlHelper;
import com.cloudbeaver.client.common.CommonUploader;
import com.cloudbeaver.client.dbbean.DatabaseBean;
import com.cloudbeaver.client.dbbean.MultiDatabaseBean;
import com.cloudbeaver.client.dbbean.TableBean;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class DbUploader extends CommonUploader{
    private static Logger logger = Logger.getLogger(DbUploader.class);

    private Map<String, String> conf;
    private String taskJson;
    private MultiDatabaseBean dbBeans;

	private int WEB_DB_UPDATE_INTERVAL = 24 * 3600 * 1000;

    private static Map<String, String> appKeySecret = new HashMap<String, String>();
    private static String appPreDefKey = "tmpKey";
    private static String appPreDefSecret = "tmpSecret";

	private static final String DB_ROW_VERSION_START_TIME = "starttime";

    //for test
    public Map<String, String> getConf() {
		return conf;
	}

    public String getTaskJson() {
		return taskJson;
	}

	public void setTaskJson(String taskJson) {
		this.taskJson = taskJson;
	}
	
	//for test
	public MultiDatabaseBean getDbBeans(){
		return dbBeans;
	}

	@Override
	public void setup() throws BeaverFatalException {
		appKeySecret.put(appPreDefKey, appPreDefSecret);

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
				BeaverUtils.printLogExceptionAndSleep(e, "get tasks failed, url:" + conf.get(CONF_TASK_SERVER_URL) + " json:" + getTaskJson() + " msg:", 60 * 1000);
			}
        }
	}

	@Override
	public int getThreadNum() {	
		ArrayList<DatabaseBean> databases = new ArrayList<DatabaseBean>();
		for (DatabaseBean dbBean : dbBeans.getDatabases()) {
			if (dbBean.getType().equals(DB_TYPE_WEB_SERVICE)) {
				DatabaseBean newBean;
				try {
					newBean = BeaverUtils.cloneTo(dbBean);
					for (TableBean tableBean : newBean.getTables()) {
						tableBean.setSyncTypeOnceADay(true);
						long now = System.currentTimeMillis();
						tableBean.setXgsjwithLong((now - now % (24 * 3600 * 1000))- WEB_DB_UPDATE_INTERVAL * 3 - 8 * 3600 * 1000);
					}
					databases.add(newBean);
				} catch (ClassNotFoundException | IOException e) {
					BeaverUtils.printLogExceptionAndSleep(e, "can't clone databaseBean,", 100);
					break;
				}
			}
		}		
		dbBeans.getDatabases().addAll(databases);
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
	public void doTask(Object taskObject) throws BeaverFatalException {
		DatabaseBean dbBean = (DatabaseBean)taskObject;
		dbBean.setQueryTime((new Date()).toString());

        for (TableBean tableBean : dbBean.getTables()) {
//        	sleep 1s when move to a new table
        	BeaverUtils.sleep(1000);

            if (tableBean.isSyncTypeOnceADay() && !tableBean.getPrevxgsj().equals(tableBean.getXgsj())) {
//				has moved to next day
            	if (System.currentTimeMillis() - tableBean.getPrevxgsjAsLong() > 3 * WEB_DB_UPDATE_INTERVAL) {
					tableBean.setPrevxgsj(tableBean.getXgsj());
				}else {
					BeaverUtils.sleep(60 * 1000);
					continue;
				}
			}

//        	keep trying until get all data from this table
        	while (true) {
        		BeaverUtils.sleep(100);

        		Date date = new Date();
        		dbBean.setQueryTime(date.toString());
        		tableBean.setQueryTime(date.toString());

        		String dbData = null;
    	        try {
    	        	if (dbBean.getType().equals(DB_TYPE_SQL_SERVER)) {
    	        		dbData = getDataFromSqlServer(dbBean, tableBean);
    	        	} else if (dbBean.getType().equals(DB_TYPE_SQL_ORACLE)) {
    	        		dbData = getDataFromOracle(dbBean, tableBean);
    	        	} else if (dbBean.getType().equals(DB_TYPE_WEB_SERVICE) && dbBean.getRowversion().equals(DB_ROW_VERSION_START_TIME)) {
    	        		dbData = getDataFromWebService(dbBean, tableBean);
    	        	}else {
    					throw new BeaverFatalException("db type is wrong, type can only be 'sqldb' or 'urldb'");
    				}
				} catch (BeaverTableIsFullException e) {
//					move next table
					break;
				} catch (BeaverTableNeedRetryException e) {
//					retry this table
					tableBean.rollBackXgsj();
					continue;
				}

                try {
                	String flumeJson = BeaverUtils.compressAndFormatFlumeHttp(dbData);
    				BeaverUtils.doPost(conf.get(CONF_FLUME_SERVER_URL), flumeJson);
    				logger.debug("send db data to flume server, json:" + flumeJson);
    			} catch (IOException e) {
//    				change back 'xgsj'
    				tableBean.rollBackXgsj();
    				BeaverUtils.printLogExceptionAndSleep(e, "post json to flume server failed, server:" + conf.get(CONF_FLUME_SERVER_URL), 1000);
    			}
			}
        }
	}

	public String getDataFromWebServiceForTest(DatabaseBean dbBean,TableBean tableBean) throws BeaverTableIsFullException, BeaverTableNeedRetryException, BeaverFatalException {
		return getDataFromWebService(dbBean,tableBean);
	}
	
	private String getDataFromWebService(DatabaseBean dbBean,TableBean tableBean) throws BeaverTableIsFullException, BeaverTableNeedRetryException, BeaverFatalException {
		String webUrl = getDBDataServerUrl(dbBean.getDbUrl(), tableBean.getTable());
		logger.debug("requet one weburl, webUrl:" + webUrl);

		while ((System.currentTimeMillis() - Long.parseLong(tableBean.getXgsj())) > WEB_DB_UPDATE_INTERVAL) {
//			loop until get some data from web server, or until today's 00:00:00
			try {
				StringBuilder sb = getDataOfSomeDay(webUrl, dbBean, tableBean);

				int totalPageThisDay = BeaverUtils.getNumberFromStringBuilder(sb, "\"totalPages\":");
				if(totalPageThisDay == -1) {
//					get error when query this page, try again
					BeaverUtils.sleep(500);
					continue;
				}

				if (totalPageThisDay == 0) {
					tableBean.moveToNextXgsj(WEB_DB_UPDATE_INTERVAL);
					BeaverUtils.sleep(500);
					continue;
				}

				tableBean.setTotalPageNum(totalPageThisDay);
				tableBean.setCurrentPageNum(BeaverUtils.getNumberFromStringBuilder(sb, "\"pageNo\":"));
				if (tableBean.getTotalPageNum() == tableBean.getCurrentPageNum()) {
//					has get all of data in this day, or there is no data in this day, move to next day
					tableBean.moveToNextXgsj(WEB_DB_UPDATE_INTERVAL);
					BeaverUtils.sleep(500);
				}

				logger.info("web query finished, time:" + tableBean.getXgsj() + " currentPage:" + tableBean.getCurrentPageNum() + " totalPage:" + tableBean.getTotalPageNum());
				return sb.toString();
			} catch (NoSuchAlgorithmException e) {
				BeaverUtils.PrintStackTrace(e);
				throw new BeaverFatalException("no md5 algorithm, exit. msg:" + e.getMessage());
			} catch (IOException | NumberFormatException e) {
				BeaverUtils.printLogExceptionAndSleep(e, "get ioexception when request data, url:" + webUrl + " msg:", 500);
				throw new BeaverTableNeedRetryException();
			}
		}

//		can't get any data since some day
		throw new BeaverTableIsFullException();
	}

	private StringBuilder getDataOfSomeDay(String webUrl, DatabaseBean dbBean,TableBean tableBean) throws NoSuchAlgorithmException, IOException {
		Map<String, String> paraMap = new HashMap<String, String>();
		paraMap.put("appkey", dbBean.getAppKey());

		if (tableBean.getCurrentPageNum() != 0) {
			paraMap.put("pageno", "" + tableBean.getCurrentPageNum() + 1);
		}

		if (dbBean.getDb().equals("PrasDB") && tableBean.getTable().equals("pras/getTable")) {
			paraMap.put("pagesize", "" + 1);
		}else {
			paraMap.put("pagesize", "" + DB_QEURY_LIMIT_WEB_SERVICE);
			paraMap.put("starttime", BeaverUtils.timestampToDateString(tableBean.getXgsj()));
			paraMap.put("endtime", BeaverUtils.timestampToDateString(Long.parseLong(tableBean.getXgsj()) + WEB_DB_UPDATE_INTERVAL));
		}

		String sign = BeaverUtils.getRequestSign(paraMap, dbBean.getAppSecret());
		paraMap.put("sign", sign);
		StringBuilder sb = BeaverUtils.doPost(webUrl, paraMap, "application/x-www-form-urlencoded");
		return sb;
	}

	private String getDataFromOracle(DatabaseBean dbBean, TableBean tableBean) throws BeaverFatalException, BeaverTableIsFullException, BeaverTableNeedRetryException {
		logger.info("Executing query : " + tableBean.getSqlString(prisonId, dbBean.getDb(), dbBean.getRowversion(), dbBean.getType(), DB_QEURY_LIMIT_DB));

		try {
			if (tableBean.getMaxXgsj().equals(CommonUploader.DB_EMPTY_ROW_VERSION) || tableBean.getMaxXgsj().equals(tableBean.getXgsj())) {
//				thus, table is empty, or table is full
				String maxRowVersion = SqlHelper.getMaxRowVersion(dbBean, tableBean);
				if (maxRowVersion.equals(CommonUploader.DB_EMPTY_ROW_VERSION) || maxRowVersion.equals(tableBean.getXgsj())) {
//					empty table, move to next table
					throw new BeaverTableIsFullException();
				}
				tableBean.setMaxXgsj(maxRowVersion);
			}

			JSONArray jArray = new JSONArray();
			while (jArray.isEmpty()) {
				String nowMaxXgsj = SqlHelper.getDBData(prisonId, dbBean, tableBean, DB_QEURY_LIMIT_DB, jArray);
				if (nowMaxXgsj.equals(CommonUploader.DB_EMPTY_ROW_VERSION)) { // && storedMaxXgsj < maxRV
					tableBean.setXgsj((tableBean.getXgsjAsLong() + DB_QEURY_LIMIT_DB) + "");
				}else {
					logger.debug("get db data, json:" + jArray.toString());
					tableBean.setXgsj(nowMaxXgsj);
					break;
				}
			}

			logger.info("get data from oracle, data:" + jArray.toString());
			return jArray.toString();
		} catch (SQLException e) {
			BeaverUtils.printLogExceptionAndSleep(e, "sql query faild, url:" + conf.get("db." + dbBean.getDb() + ".url") + " msg:", 1000);
			SqlHelper.removeConnection(dbBean);
			throw new BeaverTableNeedRetryException();
		}
	}

	private String getDataFromSqlServer(DatabaseBean dbBean, TableBean tableBean) throws BeaverFatalException, BeaverTableIsFullException, BeaverTableNeedRetryException {
		logger.info("Executing query : " + tableBean.getSqlString(prisonId, dbBean.getDb(), dbBean.getRowversion(), dbBean.getType(), DB_QEURY_LIMIT_DB));

		try {
			JSONArray jArray = new JSONArray();
			String maxXgsj = SqlHelper.getDBData(prisonId, dbBean, tableBean, DB_QEURY_LIMIT_DB, jArray);
			if (jArray.isEmpty()) {
//				move to next table
				throw new BeaverTableIsFullException();
			}else {
//				timestamp is like '0x111'
		    	if (!maxXgsj.startsWith("0x")) {
					maxXgsj = "0x" + maxXgsj;
				}
				tableBean.setXgsj(maxXgsj);
			}

			logger.debug("get db data, json:" + jArray.toString()); 
			return jArray.toString();
		} catch (SQLException e) {
			BeaverUtils.printLogExceptionAndSleep(e, "sql query faild, url:" + conf.get("db." + dbBean.getDb() + ".url") + " msg:", 1000);
			SqlHelper.removeConnection(dbBean);
			throw new BeaverTableNeedRetryException();
		}
	}

	private String getDBDataServerUrl(String dbUrl, String table) {
		return dbUrl.replaceAll("\\{tableName\\}", table);
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
        	if (dbType.equals(DB_TYPE_SQL_ORACLE) || dbType.equals(DB_TYPE_SQL_SERVER) || dbType.equals(DB_TYPE_SQL_SQLITE) || dbType.equals(DB_TYPE_WEB_SERVICE)) {
        		dbBean.setType(dbType);
			}else {
				throw new BeaverFatalException("dbtype set error, only 'urldb' or 'sqldb'");
			}

			String appKey = conf.get("db." + dbBean.getDb() + ".appKey");
			if (appKey != null) {
				dbBean.setAppKey(appKey);
				dbBean.setAppSecret(appKeySecret.get(appKey));
			}
        }
    }

    public static void main(String[] args) {
    	startDbUploader();
    }
}
