package com.cloudbeaver;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.cloudbeaver.client.common.BeaverFatalException;
import com.cloudbeaver.client.fileUploader.DirInfo;
import com.cloudbeaver.client.fileUploader.FileUploader;
import com.cloudbeaver.mockServer.MockWebServer;

public class FileUploaderTest extends FileUploader{
	private static MockWebServer mockServer = new MockWebServer();

//	@BeforeClass
	@Before
//	@Ignore
	public void setUpServers(){
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

	@Test
//	@Ignore
    public void testGetMsgForFile() throws BeaverFatalException{
        setup();
//        System.out.println(getThreadNum());
        for (int index = 0; index < getThreadNum(); index++) {
        	DirInfo  dirInfo = (DirInfo) getTaskObject(index);
            if (dirInfo == null) {
                continue;
            }
            doTask(dirInfo);
        }
    }

	public static void main(String[] args) {
		FileUploaderTest appFileTest = new FileUploaderTest();
		appFileTest.setUpServers();
		try {
			appFileTest.testGetMsgForFile();
		} catch (BeaverFatalException e) {
			e.printStackTrace();
		}
		appFileTest.tearDownServers();
	}

}
