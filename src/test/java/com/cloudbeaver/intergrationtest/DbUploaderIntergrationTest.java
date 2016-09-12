package com.cloudbeaver.intergrationtest;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.cloudbeaver.DbUploaderTest;
import com.cloudbeaver.client.common.BeaverFatalException;
import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.client.common.SqlHelper;
import com.cloudbeaver.client.dbUploader.DbUploader;
import com.cloudbeaver.client.dbbean.DatabaseBean;
import com.cloudbeaver.client.dbbean.MultiDatabaseBean;
import com.cloudbeaver.client.dbbean.TableBean;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class DbUploaderIntergrationTest extends DbUploader {
//	@Test
//	@Ignore
	public void testGetMsgProduct() throws BeaverFatalException, SQLException, JsonProcessingException, IOException{
		setup();
		MultiDatabaseBean olddbs = getDbBeans();
		int num = getDbBeans().getDatabases().size();
		MultiDatabaseBean newdbs = olddbs;
		for(int j = 0; j < DbUploaderTest.MAX_LOOP_NUM; j++){
			DbUploaderTest.isEqulas(olddbs, newdbs);
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

		                try {
		    				if (maxVersion != null) {
		    					DbUploaderTest.updateRefTask(maxVersion,olddbs,index,tBean,DbUploaderTest.DB2TypeMap.get(dbName));
		    					tBean.setXgsj("0x"+maxVersion);
			    				BeaverUtils.doPost(getConf().getFlumeServerUrl(), jArray.toString());
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

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
