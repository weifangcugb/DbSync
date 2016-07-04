package com.cloudbeaver;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.client.common.SqlHelper;
import com.cloudbeaver.client.dbUploader.DbUploader;
import com.cloudbeaver.client.dbbean.DatabaseBean;
import com.cloudbeaver.client.dbbean.MultiDatabaseBean;
import com.cloudbeaver.client.dbbean.TableBean;
import com.cloudbeaver.mockServer.MockSqlHelper;
import com.cloudbeaver.mockServer.MockSqlServer;
import com.cloudbeaver.mockServer.MockWebServer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

//@Ignore
public class AppTest{
	private static MockWebServer mockServer = new MockWebServer();
	private static MockSqlServer mockSqlServer = new MockSqlServer();
	public static String DEFAULT_CHARSET = "utf-8";

//	@BeforeClass
	@Ignore
	public static void setUpServers(){
		//start the mocked SqlServer
//		mocksqlserver.
		//start the mocked web server
		mockServer.start(false);
	}

//	@AfterClass
	@Ignore
	public static void tearDownServers(){
		mockServer.stop();
	}

	@Test
	public void testGetMsg() throws Exception {
		DbUploader dbUploader = new DbUploader();
		dbUploader.setup();

		MultiDatabaseBean olddbs = dbUploader.getDbBeans();
		MultiDatabaseBean newdbs = olddbs;
		for(int j = 0;j<5;j++){			
			isEqulas(olddbs, newdbs);
			for (int index = 0; index < dbUploader.getThreadNum(); index++) {
				DatabaseBean dbBean = (DatabaseBean) dbUploader.getTaskObject(index);
				if (dbBean == null) {
					continue;
				}
				for (TableBean tBean : dbBean.getTables()) {
					JSONArray jArray = new JSONArray();
					String maxVersion = null;
					maxVersion = MockSqlHelper.execSqlQuery(DbUploader.getPrisonId(), dbBean, tBean, dbUploader, 2, jArray);

//					jArray : [{"hdfs_client":"1","hdfs_db":"DocumentDB", xxx}]
					ObjectMapper oMapper = new ObjectMapper();
					JsonNode root = oMapper.readTree(jArray.toString());
					for (int i = 0; i < root.size(); i++) {
						JsonNode item = root.get(i);
						Assert.assertEquals(item.get("hdfs_prison").asText(),DbUploader.getPrisonId());
						Assert.assertEquals(item.get("hdfs_db").asText(),"DocumentDB");
					}
					
					if(!jArray.isEmpty()){		
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
		    					updateRefTask(maxVersion,olddbs,index,tBean);
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
		Assert.assertEquals(olddbs.getDatabases().size(), newdbs.getDatabases().size());
		for (int index = 0; index < olddbs.getDatabases().size(); index++) {
			DatabaseBean db1 = (DatabaseBean) olddbs.getDatabases().get(index);
			DatabaseBean db2 = (DatabaseBean) newdbs.getDatabases().get(index);
			Assert.assertEquals(db1.getDb(), db2.getDb());
			Assert.assertEquals(db1.getTables().size(), db2.getTables().size());
			for (int i = 0;i<db1.getTables().size();i++) {
				TableBean t1 = db1.getTables().get(i);
				TableBean t2 = db2.getTables().get(i);
				Assert.assertEquals(t1.getTable(), t2.getTable());
				Assert.assertEquals(t1.getXgsj(), t2.getXgsj());
			}
		}
	}

	public static void updateRefTask(String maxVersion, MultiDatabaseBean dbs, int index, TableBean tBean){
		DatabaseBean dbBean = dbs.getDatabases().get(index);
		for(int i = 0;i<dbBean.getTables().size();i++){
			TableBean tableBean = dbBean.getTables().get(i);
			if(tableBean.getTable().equals(tBean.getTable())){
				String xgsj = tableBean.getXgsj().substring("0x".length());
				Assert.assertTrue("max xgsj is less than old xgsj", Long.parseLong(maxVersion) > Long.parseLong(xgsj));
				tableBean.setXgsj(maxVersion);
				return;
			}
		}
	}

	
	@Test
    public void testGetMsgForWeb() throws Exception {
		DbUploader dbUploader = new DbUploader();
        dbUploader.setup();
        for (int index = 0; index < dbUploader.getThreadNum(); index++) {
            DatabaseBean dbBean = (DatabaseBean) dbUploader.getTaskObject(index);
            if (dbBean == null) {
                continue;
            }
            if(!dbBean.getType().equals(dbUploader.DB_TYPE_WEB_SERVICE))
            	continue;
            for (TableBean tBean : dbBean.getTables()) {
                String dbData = null;
                if(dbBean.getType().equals(dbUploader.DB_TYPE_WEB_SERVICE) && tBean.getTable().equals("pias/getItlist")){
                	dbData = dbUploader.getDataFromWebServiceForTest(dbBean, tBean);
                	System.out.println(dbData);
//                	return;
                }
//                JSONArray jArray = new JSONArray();
//                String maxVersion = SqlHelper.getDBData(dbUploader.getPrisonId(), dbBean, tBean, 1, jArray);
////                  jArray : [{"hdfs_client":"1","hdfs_db":"DocumentDB", xxx}]
//                ObjectMapper oMapper = new ObjectMapper();
//                JsonNode root = oMapper.readTree(jArray.toString());
//                for (int i = 0; i < root.size(); i++) {
//                    JsonNode item = root.get(i);
//                    Assert.assertEquals(item.get("hdfs_prison").asText(),dbUploader.getPrisonId());
//                    Assert.assertEquals(item.get("hdfs_db").asText(),"DocumentDB");
//                }
            }
        }
	}
	
    @Test
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
//                  jArray : [{"hdfs_client":"1","hdfs_db":"DocumentDB", xxx}]
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
//		appTest.testGetMsg();
//		appTest.testGetMsgProduct();
		appTest.testGetMsgForWeb();
	}
}
