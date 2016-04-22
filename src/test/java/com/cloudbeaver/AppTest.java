package com.cloudbeaver;

import net.sf.json.JSONArray;

import org.junit.Test;

import com.cloudbeaver.client.common.SqlHelper;
import com.cloudbeaver.client.dbUploader.DbUploader;
import com.cloudbeaver.client.dbbean.DatabaseBean;
import com.cloudbeaver.client.dbbean.TableBean;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import junit.framework.TestCase;

public class AppTest extends TestCase {
	@Test
	public void testGetMsg() throws Exception {
		DbUploader dbUploader = new DbUploader();
		dbUploader.setup();

		for (int index = 0; index < dbUploader.getThreadNum(); index++) {
			DatabaseBean dbBean = (DatabaseBean) dbUploader.getTaskObject(index);
			if (dbBean == null) {
				continue;
			}

			for (TableBean tBean : dbBean.getTables()) {
				JSONArray jArray = new JSONArray();
				String maxVersion = SqlHelper.execSqlQuery(dbBean, tBean, dbUploader, 1, jArray);

//				jArray : [{"hdfs_prison":"1","hdfs_db":"DocumentDB", xxx}]
				ObjectMapper oMapper = new ObjectMapper();
				JsonNode root = oMapper.readTree(jArray.toString());
				for (int i = 0; i < root.size(); i++) {
					JsonNode item = root.get(i);
					assertEquals(item.get("hdfs_prison").asInt(), 1);
					assertEquals(item.get("hdfs_db").asText(), "DocumentDB");
				}
			}
		}
	}

	@Test
	public void testJsonArrayEmpty(){
		JSONArray jArray = new JSONArray();
		assertEquals(jArray.toString(), "[]");
	}
}
