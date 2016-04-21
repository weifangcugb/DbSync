package com.cloudbeaver.client.dbUploader;

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
	private final static String CONFIG_FILE_NAME = "SyncClient.properties";
    private final static String TASK_SERVER_URL = "tasks-server.url";
    private static String FILE_UPLOAD_DB_NAME = "DocumentFiles";

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
        String flumeJson;
		try {
			flumeJson = getDbUploadData(dbBean);
			BeaverUtils.doPost(conf.get("flume-server.url"), flumeJson);
		} catch (SQLException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("sql query faild, msg:" + e.getMessage() + " url:" + conf.get("flume-server.url"));
		} catch (IOException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("post faild, msg:" + e.getMessage() + " url:" + conf.get("flume-server.url"));
		}
	}

    public String queryDb(DatabaseBean dbBean) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (TableBean tableBean : dbBean.getTables()) {
            logger.debug("Executing query : " + tableBean.getSqlString(dbBean.getPrison(), dbBean.getDb(), dbBean.getRowversion()));

            JsonAndList jsonAndList = SqlHelper.extractJsonAndList(dbBean, tableBean, this);
            if (jsonAndList == null) continue;

            String res = jsonAndList.getJson();
            if (res.length() > 2) {
                sb.append(res.substring(1, res.length()-1))
                        .append(',');
                        //.append(',').append('\n');
            }

//            if (jsonAndList != null &&
//                    jsonAndList.getList() != null &&
//                    jsonAndList.getList().size() > 0) {
//                tableBean.setXgsj(jsonAndList.getList().get(0).get("max_" + dbBean.getRowversion()));
//            }
        }
        if (sb.charAt(sb.length() - 1) == ',') {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append(']');
        return sb.toString();
    }
    
	public String getDbUploadData(DatabaseBean dbBean) throws SQLException {
		String dbInfo = queryDb(dbBean);
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