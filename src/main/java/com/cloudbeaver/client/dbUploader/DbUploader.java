package com.cloudbeaver.client.dbUploader;

import net.sf.json.JSONArray;

import org.apache.log4j.*;

import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.client.common.FixedNumThreadPool;
import com.cloudbeaver.client.common.SqlHelper;
import com.cloudbeaver.client.dbbean.DatabaseBean;
import com.cloudbeaver.client.dbbean.MultiDatabaseBean;
import com.cloudbeaver.client.dbbean.TableBean;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

public class DbUploader extends FixedNumThreadPool{
	private final static String CONF_CLIENT_ID = "client.id";
	private final static String CONF_FILE_NAME = "SyncClient.properties";
	private final static String CONF_FLUME_SERVER_URL = "flume-server.url";
    private final static String CONF_TASK_SERVER_URL = "tasks-server.url";

    private final static String FILE_UPLOAD_DB_NAME = "DocumentFiles";

    private final static int sqlLimitNum = 1;
    
    private static Logger logger = Logger.getLogger(DbUploader.class);

    private Map<String, String> conf = null;
    private String taskJson = null;
    private MultiDatabaseBean dbBeans = null;

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
	public void setup() {
        try {
			conf = BeaverUtils.loadConfig(CONF_FILE_NAME);
	        if (conf.containsKey(CONF_CLIENT_ID)) {
				setClientId(conf.get(CONF_CLIENT_ID));
			}else {
				logger.fatal("no client.id in config file");
				return;
			}
		} catch (IOException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.fatal("load config failed, please restart process. confName:" + CONF_FILE_NAME + " msg:" + e.getMessage());
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
				logger.error("get tasks failed, url:" + CONF_TASK_SERVER_URL + " msg:" + e.getMessage() + " json:" + getTaskJson());
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
		return dbBean.getDb().equals(FILE_UPLOAD_DB_NAME);
	}

	@Override
	public void doTask(Object taskObject) {
		DatabaseBean dbBean = (DatabaseBean)taskObject;

        for (TableBean tableBean : dbBean.getTables()) {
//        	keep trying until get some data from table
        	while (true) {
//                logger.debug("Executing query : " + tableBean.getSqlString(dbBean.getPrison(), dbBean.getDb(), dbBean.getRowversion(), sqlLimitNum));
                JSONArray jArray = new JSONArray();
                String maxXgsj = null;
				try {
					maxXgsj = SqlHelper.execSqlQuery(dbBean, tableBean, this, sqlLimitNum, jArray);
				} catch (SQLException e) {
					BeaverUtils.PrintStackTrace(e);
					logger.error("sql query faild, msg:" + e.getMessage() + " url:" + conf.get("flume-server.url"));
					SqlHelper.removeConnection(dbBean);
					BeaverUtils.sleep(1000);
					continue;
				}

				if (maxXgsj == null) {
//					time to exit, user ask to exist
					return;
				}

                if (jArray.isEmpty()) {
//	                no new record, next table
    				break;
    			}

                logger.debug("get db data, json:" + jArray.toString());

                String flumeJson = null;
				try {
					flumeJson = BeaverUtils.compressAndFormatFlumeHttp(jArray.toString());
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
    				logger.info("send db data to flume server, json:" + flumeJson);
    			} catch (IOException e) {
    				BeaverUtils.PrintStackTrace(e);
    				logger.error("post json to flume server failed, server:" + conf.get(CONF_FLUME_SERVER_URL) + " json:" + flumeJson);
    			}
			}
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
        String json = BeaverUtils.doGet(conf.get(CONF_TASK_SERVER_URL) + clientId);
        logger.debug("fetch tasks, tasks:" + json);

        setTaskJson(json);

        ObjectMapper objectMapper = new ObjectMapper();
        dbBeans = objectMapper.readValue(taskJson, MultiDatabaseBean.class);
        for (DatabaseBean dbBean : dbBeans.getDatabases()) {
        	dbBean.setDbUrl(conf.get("db." + dbBean.getDb() + ".url"));
        	dbBean.setDbUserName(conf.get("db." + dbBean.getDb() + ".username"));
        	dbBean.setDbPassword(conf.get("db." + dbBean.getDb() + ".password"));
        }
    }

    public static void main(String[] args) {
    	startDbUploader();
    }
}