package com.cloudbeaver;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.cloudbeaver.client.common.BeaverFatalException;
import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.client.common.SqlHelper;
import com.cloudbeaver.client.dbUploader.DbUploader;
import com.cloudbeaver.client.dbbean.DatabaseBean;
import com.cloudbeaver.client.dbbean.MultiDatabaseBean;
import com.cloudbeaver.client.dbbean.TableBean;
import com.cloudbeaver.mockServer.MockSqlServer;
import com.cloudbeaver.mockServer.MockWebServer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 
 * @author beaver
 * This is mainly test DbUploader
 * testGetMsgForSqlserverStep():test SqlServer. Execute one sql statement each time to make sure that the task has been updated
 * testGetMsgForSqlserver():test SqlServer. Query all the tables
 * public void testGetMsgForSqlite():test Sqlite. Execute one sql statement each time to make sure that the task has been updated
 * testGetMsgForWeb():test web service, not including databases from three days before
 * testGetMsgForWebSync():test web service, only testing databases from three days before
 * testGetMsgForOracle():test oracle.
 * testGetMsgProduct():test all kinds of databases togeher
 *
 */

//@Ignore
public class DbUploaderTest extends DbUploader{
	public static final int MAX_LOOP_NUM = 5;
	public static String DEFAULT_CHARSET = "utf-8";
	public static Map<String, String> DB2TypeMap = new HashMap<String, String>();
	{
		DB2TypeMap.put("DocumentDB", "sqlserver");
		DB2TypeMap.put("MeetingDB", "webservice");
		DB2TypeMap.put("TalkDB", "webservice");
		DB2TypeMap.put("PrasDB", "webservice");
		DB2TypeMap.put("JfkhDB", "oracle");
		DB2TypeMap.put("DocumentDBForSqlite", "sqlite");
		DB2TypeMap.put("DocumentFiles", "file");
		DB2TypeMap.put("VideoMeetingDB", "sqlserver");
		DB2TypeMap.put("HelpDB", "sqlserver");
	}

	private static MockWebServer mockServer = new MockWebServer();
	private static MockSqlServer mockSqlServer = new MockSqlServer();

//	@BeforeClass
	@Before
//	@Ignore
	public void setUpServers(){
		//start the mocked SqlServer
//		mocksqlserver.
		//start the mocked web server
		mockServer.start(false);
	}

//	@AfterClass
	@After
//	@Ignore
	public void tearDownServers(){
		mockServer.stop();
	}

	@Override
	protected void handleBeaverFatalException(BeaverFatalException e) throws Exception {
		super.handleBeaverFatalException(e);
		throw new Exception("get BeaverFatalException during testing");
	}

//	@Test
	@Ignore
	public void testGetMsgForSqlserverStep() throws Exception {
		setup();
		MultiDatabaseBean olddbs = getDbBeans();
		int num = getDbBeans().getDatabases().size();
		MultiDatabaseBean newdbs = olddbs;
		for(int j = 0; j < MAX_LOOP_NUM; j++){
			isEqulas(olddbs, newdbs);			
			for (int index = 0; index < num; index++) {
				DatabaseBean dbBean = (DatabaseBean) getTaskObject(index);
				if (dbBean == null) {
					continue;
				}
				if(!dbBean.getType().equals(DbUploader.DB_TYPE_SQL_SERVER)){
					continue;
				}
				for (TableBean tBean : dbBean.getTables()) {
					JSONArray jArray = new JSONArray();
					String maxVersion = null;
					//test sqlserver
					maxVersion = SqlHelper.getDBData(prisonId, dbBean, tBean, 2, jArray);

//					jArray : [{"hdfs_client":"1","hdfs_db":"DocumentDB", xxx}]
					ObjectMapper oMapper = new ObjectMapper();
					JsonNode root = oMapper.readTree(jArray.toString());
					for (int i = 0; i < root.size(); i++) {
						JsonNode item = root.get(i);
						Assert.assertEquals(item.get("hdfs_prison").asText(),prisonId);
						Assert.assertEquals(item.get("hdfs_db").asText(),"DocumentDB");
					}
					
					if(!jArray.isEmpty()){	
						JSONArray newjArray = JSONArray.fromObject(jArray.toArray());						
						String dbName = null;
						if(newjArray.size()>0){
							JSONObject job = newjArray.getJSONObject(0);
							dbName = (String) job.get("hdfs_db");
						}
		                String flumeJson = null;
						try {
							flumeJson = BeaverUtils.compressAndFormatFlumeHttp(jArray.toString());
						} catch (IOException e) {
							BeaverUtils.PrintStackTrace(e);
							BeaverUtils.sleep(1000);
							continue;
						}					
		                try {
		    				if (maxVersion != null) {
		    					updateRefTask(maxVersion,olddbs,index,tBean,DB2TypeMap.get(dbName));
		    					tBean.setXgsj("0x"+maxVersion);
			    				BeaverUtils.doPost(getFLumeUrl(), flumeJson);
			    				//test HeartBeat
			    				doHeartBeat();
			    				break;
							}
		    			} catch (IOException e) {
		    				BeaverUtils.PrintStackTrace(e);
		    				BeaverUtils.sleep(1000);
		    			}
					}
				}
			}
			setup();
			newdbs = getDbBeans();
		}
	}

//	@Test
	@Ignore
    public void testGetMsgForSqlserver() throws Exception {
        setup();
        int num = getThreadNum();
        for (int index = 0; index < num; index++) {
            DatabaseBean dbBean = (DatabaseBean) getTaskObject(index);
            if (dbBean == null) {
                continue;
            }
            if(!dbBean.getType().equals(DbUploader.DB_TYPE_SQL_SERVER)){
            	continue;
            }
            //test sqlserver
            doTask(dbBean);
        }
	}

	@Test
//	@Ignore
	public void testGetMsgForSqlite() throws Exception {
		setup();
		MultiDatabaseBean olddbs = getDbBeans();
		int num = getDbBeans().getDatabases().size();
		MultiDatabaseBean newdbs = olddbs;
		for(int j = 0; j < MAX_LOOP_NUM; j++){
			isEqulas(olddbs, newdbs);
			for (int index = 0; index < num; index++) {
				DatabaseBean dbBean = (DatabaseBean) getTaskObject(index);
				if (dbBean == null) {
					continue;
				}
				if(!dbBean.getType().equals(DbUploader.DB_TYPE_SQL_SQLITE)){
					continue;
				}
				for (TableBean tBean : dbBean.getTables()) {
					JSONArray jArray = new JSONArray();
					String maxVersion = null;
					//test sqlite
					maxVersion = SqlHelper.getDBData(prisonId, dbBean, tBean, 2, jArray);

//					jArray : [{"hdfs_client":"1","hdfs_db":"DocumentDB", xxx}]
					ObjectMapper oMapper = new ObjectMapper();
					JsonNode root = oMapper.readTree(jArray.toString());
					for (int i = 0; i < root.size(); i++) {
						JsonNode item = root.get(i);
						Assert.assertEquals(item.get("hdfs_prison").asText(), prisonId);
						Assert.assertEquals(item.get("hdfs_db").asText(),"DocumentDBForSqlite");
					}
					
					if(!jArray.isEmpty()){	
						JSONArray newjArray = JSONArray.fromObject(jArray.toArray());						
						String dbName = null;
						if(newjArray.size()>0){
							JSONObject job = newjArray.getJSONObject(0);
							dbName = (String) job.get("hdfs_db");
						}
		                String flumeJson = null;
						try {
							flumeJson = BeaverUtils.compressAndFormatFlumeHttp(jArray.toString());
						} catch (IOException e) {
							BeaverUtils.PrintStackTrace(e);
							BeaverUtils.sleep(1000);
							continue;
						}					
		                try {
		    				if (maxVersion != null) {
		    					updateRefTask(maxVersion,olddbs,index,tBean,DB2TypeMap.get(dbName));
		    					tBean.setXgsj(maxVersion);
			    				BeaverUtils.doPost(getFLumeUrl(), flumeJson);
			    				break;
							}
		    			} catch (IOException e) {
		    				BeaverUtils.PrintStackTrace(e);
		    				BeaverUtils.sleep(1000);
		    			}
					}
				}
			}
			setup();
			newdbs = getDbBeans();
		}
	}

	public static void isEqulas(MultiDatabaseBean olddbs, MultiDatabaseBean newdbs) {
//		Assert.assertEquals(olddbs.getDatabases().size(), newdbs.getDatabases().size());
//		System.out.println(olddbs.getDatabases().size());
//		System.out.println(newdbs.getDatabases().size());
		for (int index = 0; index < olddbs.getDatabases().size(); index++) {
			DatabaseBean db1 = (DatabaseBean) olddbs.getDatabases().get(index);
			DatabaseBean db2 = (DatabaseBean) newdbs.getDatabases().get(index);
			Assert.assertEquals(db1.getDb(), db2.getDb());
			Assert.assertEquals(db1.getTables().size(), db2.getTables().size());
			for (int i = 0;i<db1.getTables().size();i++) {
				TableBean t1 = db1.getTables().get(i);
				TableBean t2 = db2.getTables().get(i);
				Assert.assertEquals(t1.getTable(), t2.getTable());
//				System.out.println(t1.getXgsj());
//				System.out.println(t2.getXgsj());
				if(DB2TypeMap.get(db1.getDb()).equals("sqlserver") && DB2TypeMap.get(db2.getDb()).equals("sqlserver")){
					Assert.assertEquals(t1.getXgsj(), t2.getXgsj());
				}
				else if(DB2TypeMap.get(db1.getDb()).equals("webservice") && DB2TypeMap.get(db2.getDb()).equals("webservice")){
					Assert.assertEquals(t1.getStarttime(), t2.getStarttime());
				}
				else if(DB2TypeMap.get(db1.getDb()).equals("oracle") && DB2TypeMap.get(db2.getDb()).equals("oracle")){
					Assert.assertEquals(t1.getID(), t2.getID());
				}
				else if(DB2TypeMap.get(db1.getDb()).equals("sqlite") && DB2TypeMap.get(db2.getDb()).equals("sqlite")){
					Assert.assertEquals(t1.getXgsj(), t2.getXgsj());
				}
			}
		}
	}

	public static void updateRefTask(String maxVersion, MultiDatabaseBean dbs, int index, TableBean tBean, String serverType){
		DatabaseBean dbBean = dbs.getDatabases().get(index);
		for(int i = 0;i<dbBean.getTables().size();i++){
			TableBean tableBean = dbBean.getTables().get(i);
			if(tableBean.getTable().equals(tBean.getTable())){
//				String xgsj = tableBean.getXgsj().substring("0x".length());				
//				Assert.assertTrue("max xgsj is less than old xgsj", Long.parseLong(maxVersion) > Long.parseLong(xgsj));
				String xgsj = null;
				if(serverType.equals("sqlserver")){
					xgsj = tableBean.getXgsj();
					Assert.assertTrue("max xgsj is less than old xgsj", Long.parseLong(maxVersion,16) > Long.parseLong(xgsj.substring("0x".length()),16));
					tableBean.setXgsj("0x"+maxVersion);
					Assert.assertEquals(tableBean.getXgsj(), "0x"+maxVersion);
				}
				else if(serverType.equals("webservice")){
					xgsj = tableBean.getStarttime();
					Assert.assertTrue("max starttime is less than old starttime", Long.parseLong(maxVersion) > Long.parseLong(xgsj));
					tableBean.setStarttime(maxVersion);
					Assert.assertEquals(tableBean.getStarttime(), maxVersion);
				}
				else if(serverType.equals("oracle")){
					xgsj = tableBean.getID();
					Assert.assertTrue("max ID is less than old ID", Long.parseLong(maxVersion) > Long.parseLong(xgsj));
					tableBean.setID(maxVersion);
					Assert.assertEquals(tableBean.getID(), maxVersion);
				}
				else if(serverType.equals("sqlite")){
					xgsj = tableBean.getXgsj();
					Assert.assertTrue("max xgsj is less than old xgsj", Long.parseLong(maxVersion) > Long.parseLong(xgsj));
					tableBean.setXgsj(maxVersion);
					Assert.assertEquals(tableBean.getXgsj(), maxVersion);
				}
				return;
			}
		}
	}

//	@Test
	@Ignore
    public void testGetMsgForWeb() throws Exception {
        setup();
        int num = getThreadNum();
        for (int index = 0; index < num; index++) {
            DatabaseBean dbBean = (DatabaseBean) getTaskObject(index);
            if (dbBean == null) {
                continue;
            }
            if(!dbBean.getType().equals(DbUploader.DB_TYPE_WEB_SERVICE)){
            	continue;
            }
            //case 1: test day by day until yesterday
            webServiceDayByDay(dbBean);
        }
	}

//	@Test
	@Ignore
	public void testGetMsgForWebSync() throws Exception {
        setup();
        int num = getThreadNum();
        for (int index = 0; index < num; index++) {
            DatabaseBean dbBean = (DatabaseBean) getTaskObject(index);
            if (dbBean == null) {
                continue;
            }
            if(!dbBean.getType().equals(DbUploader.DB_TYPE_WEB_SERVICE)){
            	continue;
            }
            //case 2: test when SyncTypeOnceADay is true
            webServiceSyncTypeOnceADay(dbBean);
        }
	}

	public void webServiceSyncTypeOnceADay(DatabaseBean dbBean) throws BeaverFatalException{
		if(dbBean.getType().equals(DbUploader.DB_TYPE_WEB_SERVICE)){
			for (TableBean tBean : dbBean.getTables()) {
	        	if(tBean.isSyncTypeOnceADay()){
	        		doTask(dbBean);
	        		break;
	        	}
	        }
		}		
	}

	public void webServiceDayByDay(DatabaseBean dbBean) throws BeaverFatalException{
//      test by day, including cases when totalpages = 0 and pageno = 0
        if(dbBean.getType().equals(DbUploader.DB_TYPE_WEB_SERVICE)){
        	for (TableBean tBean : dbBean.getTables()) {
	        	if(!tBean.isSyncTypeOnceADay()){
	        		doTask(dbBean);
	        		break;
	        	}
	        }
        }
	}

//	@Test
	@Ignore
	public void	testGetMsgForOraleStep() throws SQLException, BeaverFatalException, JsonProcessingException, IOException {
		setup();
		MultiDatabaseBean olddbs = getDbBeans();
		int num = getDbBeans().getDatabases().size();
		MultiDatabaseBean newdbs = olddbs;
		for(int j = 0; j < MAX_LOOP_NUM; j++){
			isEqulas(olddbs, newdbs);			
			for (int index = 0; index < num; index++) {
				DatabaseBean dbBean = (DatabaseBean) getTaskObject(index);
				if (dbBean == null) {
					continue;
				}
				if(!dbBean.getType().equals(DbUploader.DB_TYPE_SQL_ORACLE)){
					continue;
				}
				for (TableBean tBean : dbBean.getTables()) {
					JSONArray jArray = new JSONArray();
					String maxVersion = null;
					//test oracle
					maxVersion = SqlHelper.getDBData(prisonId, dbBean, tBean, 2, jArray);

//					jArray : [{"hdfs_client":"1","hdfs_db":"DocumentDB", xxx}]
					ObjectMapper oMapper = new ObjectMapper();
					JsonNode root = oMapper.readTree(jArray.toString());
					for (int i = 0; i < root.size(); i++) {
						JsonNode item = root.get(i);
						Assert.assertEquals(item.get("hdfs_prison").asText(),prisonId);
						Assert.assertEquals(item.get("hdfs_db").asText(),"JfkhDB");
					}
					
					if(!jArray.isEmpty()){	
						JSONArray newjArray = JSONArray.fromObject(jArray.toArray());						
						String dbName = null;
						if(newjArray.size()>0){
							JSONObject job = newjArray.getJSONObject(0);
							dbName = (String) job.get("hdfs_db");
						}
		                String flumeJson = null;
						try {
							flumeJson = BeaverUtils.compressAndFormatFlumeHttp(jArray.toString());
						} catch (IOException e) {
							BeaverUtils.PrintStackTrace(e);
							BeaverUtils.sleep(1000);
							continue;
						}					
		                try {
		    				if (maxVersion != null) {
		    					updateRefTask(maxVersion,olddbs,index,tBean,DB2TypeMap.get(dbName));
		    					tBean.setXgsj(maxVersion);
			    				BeaverUtils.doPost(getFLumeUrl(), flumeJson);
			    				//test HeartBeat
			    				doHeartBeat();
			    				break;
							}
		    			} catch (IOException e) {
		    				BeaverUtils.PrintStackTrace(e);
		    				BeaverUtils.sleep(1000);
		    			}
					}
				}
			}
			setup();
			newdbs = getDbBeans();
		}
	}

//	@Test(timeout = 5 * 60 * 1000) 
	@Ignore
    public void testGetMsgForOracle() throws BeaverFatalException {
        setup();
        int num = getThreadNum();
        for (int index = 0; index < num; index++) {
            DatabaseBean dbBean = (DatabaseBean) getTaskObject(index);
            if (dbBean == null) {
                continue;
            }
            if(!dbBean.getType().equals(DbUploader.DB_TYPE_SQL_ORACLE)){
            	continue;
            }
            //test oracle
            doTask(dbBean);
        }
	}

    @Test
//	@Ignore
    public void testGetMsgProduct() throws BeaverFatalException{
        setup();
        int num = getThreadNum();
        for (int index = 0; index < num; index++) {
            DatabaseBean dbBean = (DatabaseBean) getTaskObject(index);
            if (dbBean == null || dbBean.getType().equals(DbUploader.DB_TYPE_SQL_SQLITE)) {
                continue;
            }else if(dbBean.getDb().equals("TalkDB")){
            	doTask(dbBean);
            }
            
        }
    }

	public static void main(String[] args) {
		DbUploaderTest appTest = new DbUploaderTest();
//		appTest.setUpServers();

		try {
//			appTest.testGetMsgForSqlserverStep();
//			appTest.testGetMsgForSqlserver();
//			appTest.testGetMsgForWeb();
//			appTest.testGetMsgForWebSync();
			appTest.testGetMsgForOracle();
//			appTest.testGetMsgForOraleStep();
//			appTest.testGetMsgForSqlite();
			appTest.testGetMsgProduct();
		} catch (Exception e) {
			e.printStackTrace();
		}
//		appTest.tearDownServers();
	}
}
