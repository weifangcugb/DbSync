package com.cloudbeaver;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

//@Ignore
public class AppTest{
	private static MockWebServer mockServer = new MockWebServer();
	private static MockSqlServer mockSqlServer = new MockSqlServer();
	public static String DEFAULT_CHARSET = "utf-8";
	public static Map<String, String> map = new HashMap<String, String>();

	{
		map.put("DocumentDB", "sqlserver");
		map.put("MeetingDB", "webservice");
		map.put("TalkDB", "webservice");
		map.put("PrasDB", "webservice");
		map.put("JfkhDB", "oracle");
		map.put("DocumentDBForSqlite", "sqlite");
	}

	@BeforeClass
//	@Ignore
	public static void setUpServers(){
		//start the mocked SqlServer
//		mocksqlserver.
		//start the mocked web server
		mockServer.start(false);
	}

	@AfterClass
//	@Ignore
	public static void tearDownServers(){
		mockServer.stop();
	}

	@Test
//	@Ignore
	public void testGetMsgForSqlserverStep() throws Exception {
		DbUploader dbUploader = new DbUploader();
		dbUploader.setup();

		MultiDatabaseBean olddbs = dbUploader.getDbBeans();
		int num = dbUploader.getDbBeans().getDatabases().size();
		MultiDatabaseBean newdbs = olddbs;
		for(int j = 0;j<2;j++){
			isEqulas(olddbs, newdbs);			
			for (int index = 0; index < num; index++) {
				DatabaseBean dbBean = (DatabaseBean) dbUploader.getTaskObject(index);
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
					maxVersion = SqlHelper.getDBData(dbUploader.getPrisonId(), dbBean, tBean, 2, jArray);

//					jArray : [{"hdfs_client":"1","hdfs_db":"DocumentDB", xxx}]
					ObjectMapper oMapper = new ObjectMapper();
					JsonNode root = oMapper.readTree(jArray.toString());
					for (int i = 0; i < root.size(); i++) {
						JsonNode item = root.get(i);
						Assert.assertEquals(item.get("hdfs_prison").asText(),DbUploader.getPrisonId());
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
		    					updateRefTask(maxVersion,olddbs,index,tBean,map.get(dbName));
		    					tBean.setXgsj("0x"+maxVersion);
			    				BeaverUtils.doPost(dbUploader.getConf().get(dbUploader.CONF_FLUME_SERVER_URL), flumeJson);
			    				//test HeartBeat
			    				dbUploader.testDoHeartBeat();
			    				break;
							}
		    			} catch (IOException e) {
		    				BeaverUtils.PrintStackTrace(e);
		    				BeaverUtils.sleep(1000);
		    			}
					}
				}
			}
			dbUploader.setup();
			newdbs = dbUploader.getDbBeans();
		}
	}

	@Test
//	@Ignore
    public void testGetMsgForSqlserver() throws Exception {
		DbUploader dbUploader = new DbUploader();
        dbUploader.setup();
        int num = dbUploader.getThreadNum();
        for (int index = 0; index < num; index++) {
            DatabaseBean dbBean = (DatabaseBean) dbUploader.getTaskObject(index);
            if (dbBean == null) {
                continue;
            }
            if(!dbBean.getType().equals(DbUploader.DB_TYPE_SQL_SERVER)){
            	continue;
            }
            //test sqlserver
            dbUploader.doTask(dbBean);
        }
	}

	@Test
//	@Ignore
	public void testGetMsgForSqlite() throws Exception {
		DbUploader dbUploader = new DbUploader();
		dbUploader.setup();

		MultiDatabaseBean olddbs = dbUploader.getDbBeans();
		int num = dbUploader.getDbBeans().getDatabases().size();
		MultiDatabaseBean newdbs = olddbs;
		for(int j = 0;j<5;j++){
			isEqulas(olddbs, newdbs);			
			for (int index = 0; index < num; index++) {
				DatabaseBean dbBean = (DatabaseBean) dbUploader.getTaskObject(index);
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
					maxVersion = SqlHelper.getDBData(dbUploader.getPrisonId(), dbBean, tBean, 2, jArray);

//					jArray : [{"hdfs_client":"1","hdfs_db":"DocumentDB", xxx}]
					ObjectMapper oMapper = new ObjectMapper();
					JsonNode root = oMapper.readTree(jArray.toString());
					for (int i = 0; i < root.size(); i++) {
						JsonNode item = root.get(i);
						Assert.assertEquals(item.get("hdfs_prison").asText(),DbUploader.getPrisonId());
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
		    					updateRefTask(maxVersion,olddbs,index,tBean,map.get(dbName));
		    					tBean.setXgsj(maxVersion);
			    				BeaverUtils.doPost(dbUploader.getConf().get(dbUploader.CONF_FLUME_SERVER_URL), flumeJson);
			    				dbUploader.testDoHeartBeat();
			    				break;
							}
		    			} catch (IOException e) {
		    				BeaverUtils.PrintStackTrace(e);
		    				BeaverUtils.sleep(1000);
		    			}
					}
				}
			}
			dbUploader.setup();
			newdbs = dbUploader.getDbBeans();
		}
	}

	public static void isEqulas(MultiDatabaseBean olddbs, MultiDatabaseBean newdbs) {
//		Assert.assertEquals(olddbs.getDatabases().size(), newdbs.getDatabases().size());
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
				if(map.get(db1.getDb()).equals("sqlserver") && map.get(db2.getDb()).equals("sqlserver")){
					Assert.assertEquals(t1.getXgsj(), t2.getXgsj());
				}
				else if(map.get(db1.getDb()).equals("webservice") && map.get(db2.getDb()).equals("webservice")){
					Assert.assertEquals(t1.getStarttime(), t2.getStarttime());
				}
				else if(map.get(db1.getDb()).equals("oracle") && map.get(db2.getDb()).equals("oracle")){
					Assert.assertEquals(t1.getID(), t2.getID());
				}
				else if(map.get(db1.getDb()).equals("sqlite") && map.get(db2.getDb()).equals("sqlite")){
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
//					System.out.println(tableBean.getXgsj());
				}
				else if(serverType.equals("webservice")){
					xgsj = tableBean.getStarttime();
					Assert.assertTrue("max starttime is less than old starttime", Long.parseLong(maxVersion) > Long.parseLong(xgsj));
					tableBean.setStarttime(maxVersion);
				}
				else if(serverType.equals("oracle")){
					xgsj = tableBean.getID();
					Assert.assertTrue("max ID is less than old ID", Long.parseLong(maxVersion) > Long.parseLong(xgsj));
					tableBean.setID(maxVersion);
				}
				else if(serverType.equals("sqlite")){
					xgsj = tableBean.getXgsj();
					Assert.assertTrue("max xgsj is less than old xgsj", Long.parseLong(maxVersion) > Long.parseLong(xgsj));
					tableBean.setXgsj(maxVersion);
				}
				return;
			}
		}
	}

	@Test
//	@Ignore
    public void testGetMsgForWeb() throws Exception {
		DbUploader dbUploader = new DbUploader();
        dbUploader.setup();
        int num = dbUploader.getThreadNum();
        for (int index = 0; index < num; index++) {
            DatabaseBean dbBean = (DatabaseBean) dbUploader.getTaskObject(index);
            if (dbBean == null) {
                continue;
            }
            if(!dbBean.getType().equals(DbUploader.DB_TYPE_WEB_SERVICE)){
            	continue;
            }
            //case 1: test day by day until yesterday
            testDayByDay(dbBean, dbUploader);
        }
	}

	@Test
//	@Ignore
	public void testGetMsgForWebSync() throws Exception {
		DbUploader dbUploader = new DbUploader();
        dbUploader.setup();
        int num = dbUploader.getThreadNum();
        for (int index = 0; index < num; index++) {
            DatabaseBean dbBean = (DatabaseBean) dbUploader.getTaskObject(index);
            if (dbBean == null) {
                continue;
            }
            if(!dbBean.getType().equals(DbUploader.DB_TYPE_WEB_SERVICE)){
            	continue;
            }
            //case 2: test when SyncTypeOnceADay is true
            testSyncTypeOnceADay(dbBean, dbUploader);
        }
	}
	
//	@Test
	@Ignore
    public void testGetMsgForOracle() throws Exception {
		DbUploader dbUploader = new DbUploader();
        dbUploader.setup();
        int num = dbUploader.getThreadNum();
        for (int index = 0; index < num; index++) {
            DatabaseBean dbBean = (DatabaseBean) dbUploader.getTaskObject(index);
            if (dbBean == null) {
                continue;
            }
            if(!dbBean.getType().equals(DbUploader.DB_TYPE_SQL_ORACLE)){
            	continue;
            }
            //test oracle
            dbUploader.doTask(dbBean);
        }
	}
	
	public static void testSyncTypeOnceADay(DatabaseBean dbBean, DbUploader dbUploader) throws BeaverFatalException{
		if(dbBean.getType().equals(DbUploader.DB_TYPE_WEB_SERVICE)){
			for (TableBean tBean : dbBean.getTables()) {
	        	if(tBean.isSyncTypeOnceADay()){
	        		dbUploader.doTask(dbBean);
	        		break;
	        	}
	        }
		}		
	}
	
	public static void testDayByDay(DatabaseBean dbBean, DbUploader dbUploader) throws BeaverFatalException{
//      test by day, including cases when totalpages = 0 and pageno = 0
        if(dbBean.getType().equals(DbUploader.DB_TYPE_WEB_SERVICE)){
        	for (TableBean tBean : dbBean.getTables()) {
	        	if(!tBean.isSyncTypeOnceADay()){
	        		dbUploader.doTask(dbBean);
	        		break;
	        	}
	        }
        }
	}

//    @Test
	@Ignore
    public void testGetMsgProduct() throws Exception {
        DbUploader dbUploader = new DbUploader();
        dbUploader.setup();
        for (int index = 0; index < dbUploader.getThreadNum(); index++) {
            DatabaseBean dbBean = (DatabaseBean) dbUploader.getTaskObject(index);
            if (dbBean == null) {
                continue;
            }
            for (TableBean tBean : dbBean.getTables()) {
                JSONArray jArray = new JSONArray();
                String maxVersion = SqlHelper.getDBData(dbUploader.getPrisonId(), dbBean, tBean, 1, jArray);
//              jArray : [{"hdfs_client":"1","hdfs_db":"DocumentDB", xxx}]
                ObjectMapper oMapper = new ObjectMapper();
                JsonNode root = oMapper.readTree(jArray.toString());
                for (int i = 0; i < root.size(); i++) {
                    JsonNode item = root.get(i);
                    Assert.assertEquals(item.get("hdfs_prison").asText(),dbUploader.getPrisonId());
                    Assert.assertEquals(item.get("hdfs_db").asText(),"DocumentDB");
                }
            }
        }
    }

	public static void main(String[] args) throws Exception {
		AppTest appTest = new AppTest();
//		AppTest.setUpServers();
//		appTest.testGetMsgForSqlserverStep();
//		appTest.testGetMsgForSqlserver();
//		appTest.testGetMsgForWeb();
//		appTest.testGetMsgForWebSync();
//		appTest.testGetMsgForOracle();
//		appTest.testGetMsgForSqlite();
//		appTest.testGetMsgProduct();
//		AppTest.tearDownServers();
	}
}
