package com.cloudbeaver;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.hamcrest.Condition.Step;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.client.common.SqlHelper;
import com.cloudbeaver.client.dbUploader.DbUploader;
import com.cloudbeaver.client.dbbean.DatabaseBean;
import com.cloudbeaver.client.dbbean.TableBean;
import com.cloudbeaver.mockServer.MockSqlServer;
import com.cloudbeaver.mockServer.MockWebServer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yammer.metrics.stats.EWMA;

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
		Map <String,String> m = new HashMap<String,String>();
		DbUploader dbUploader = new DbUploader();
		for(int j = 0;j<3;j++){
			dbUploader.setup();
			for (int index = 0; index < dbUploader.getThreadNum(); index++) {
				DatabaseBean dbBean = (DatabaseBean) dbUploader.getTaskObject(index);
				if (dbBean == null) {
					continue;
				}
				for (TableBean tBean : dbBean.getTables()) {
					JSONArray jArray = new JSONArray();
					String maxVersion = SqlHelper.execSqlQuery(dbUploader.getPrisonId(), dbBean, tBean, dbUploader, 1, jArray);

//					jArray : [{"hdfs_client":"1","hdfs_db":"DocumentDB", xxx}]
					ObjectMapper oMapper = new ObjectMapper();
					JsonNode root = oMapper.readTree(jArray.toString());
					for (int i = 0; i < root.size(); i++) {
						JsonNode item = root.get(i);
						Assert.assertEquals(item.get("hdfs_prison").asText(),dbUploader.getPrisonId());
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
		    				BeaverUtils.doPost(dbUploader.getConf().get(dbUploader.CONF_FLUME_SERVER_URL), flumeJson);
		    				if (maxVersion != null) {
		    					tBean.setXgsj(maxVersion);
		    					break;
							}
		    			} catch (IOException e) {
		    				BeaverUtils.PrintStackTrace(e);
		    				BeaverUtils.sleep(1000);
		    			}
					}
				}
			}
		}
	}

}
