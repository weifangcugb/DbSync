package com.cloudbeaver;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudbeaver.client.common.BeaverFatalException;
import com.cloudbeaver.mockServer.MockWebServer;
import com.cloudbeaver.mockServer.StandaloneJafkaServer;
import com.cloudbeaver.mockServer.StandaloneZKServer;

public class BrokerMonitorTest implements Runnable{
	private static MockWebServer mockServer = new MockWebServer();
	private static boolean HAS_ZK_BEEN_SETUP = false;

	@Before
//	@Ignore
	public void setUpServers(){
		//start the mocked web server
		mockServer.start(false);
	}

	@After
//	@Ignore
	public void tearDownServers(){
		mockServer.stop();
	}

	@Test
//	@Ignore
    public void testBrokerMonitor() throws BeaverFatalException, InterruptedException{		
		BrokerMonitorTest mt=new BrokerMonitorTest();    
        new Thread(mt).start();
        new Thread(mt).start();
    }

	public static void main(String[] args) throws BeaverFatalException, InterruptedException {
		BrokerMonitorTest brokerMonitorTest = new BrokerMonitorTest();
		brokerMonitorTest.testBrokerMonitor();
	}

	@Override
	public void run() {
		if(!HAS_ZK_BEEN_SETUP){
			StandaloneZKServer zkServer = new StandaloneZKServer();
			HAS_ZK_BEEN_SETUP = true;
			zkServer.setUpZookeeperServer();
		}
		else {
			StandaloneJafkaServer jafkaServer = new StandaloneJafkaServer();
			jafkaServer.setUpJafkaServer();
		}
	}

}
