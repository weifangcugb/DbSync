package com.cloudbeaver.intergrationtest;

import org.junit.Ignore;

import com.cloudbeaver.client.common.BeaverFatalException;
import com.cloudbeaver.client.dbUploader.DbUploader;
import com.cloudbeaver.client.dbbean.DatabaseBean;

public class DbUploaderTest extends DbUploader {

//  @Test
	@Ignore
	public void testGetMsgProduct() throws BeaverFatalException{
		setup();
		int num = getThreadNum();
		for (int index = 0; index < num; index++) {
			DatabaseBean dbBean = (DatabaseBean) getTaskObject(index);
			if (dbBean == null) {
				continue;
			}
			if(dbBean.getType().equals(DB_TYPE_SQL_SQLITE)){
				continue;
			}
			else if(dbBean.getType().equals(DB_TYPE_SQL_ORACLE)){
				continue;
			}
			else{
//				doTask(dbBean);
			}
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
