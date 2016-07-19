package com.cloudbeaver;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.mockServer.MockWebServer;
import com.cloudbeaver.mockServer.StandaloneJafkaServer;
import com.cloudbeaver.mockServer.StandaloneZKServer;

public class BrokerMonitorTest{
	private static Logger logger = Logger.getLogger(BrokerMonitorTest.class);

	private static MockWebServer mockServer = new MockWebServer();
	private static String brokerListUrl = "http://localhost:8877/getBrokerList";

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

	class ZookeeperRunner extends Thread {
		public void run() {
			StandaloneZKServer zkServer = new StandaloneZKServer();
			zkServer.startZookeeper();
		}
	}

	class JafkaRunner extends Thread {
		StandaloneJafkaServer jafkaServer = new StandaloneJafkaServer();
		public void run() {
			jafkaServer.startJafka();
		}
	}

	@Test
	public void testBrokerMonitor() throws IOException {
		ZookeeperRunner zkRunner = new ZookeeperRunner();
		zkRunner.start();
		BeaverUtils.sleep(5 * 1000);

		JafkaRunner jafkaRunner1 = new JafkaRunner();
		jafkaRunner1.start();
		BeaverUtils.sleep(5 * 1000);
		String brokerList = BeaverUtils.doGet(brokerListUrl);
		Assert.assertEquals(brokerList,"{\"error_code\":0,\"zkd\":[{\"ids\":\"1\",\"host\":\"beaver-shujie\",\"port\":9093}]}");
		System.out.println("Broker list = " + BeaverUtils.doGet(brokerListUrl));

		JafkaRunner jafkaRunner2 = new JafkaRunner();
		jafkaRunner2.start();
		BeaverUtils.sleep(5 * 1000);
		brokerList = BeaverUtils.doGet(brokerListUrl);
		Assert.assertEquals(brokerList,"{\"error_code\":0,\"zkd\":[{\"ids\":\"2\",\"host\":\"beaver-shujie\",\"port\":9094},{\"ids\":\"1\",\"host\":\"beaver-shujie\",\"port\":9093}]}");
		System.out.println("Broker list = " + brokerList);

		jafkaRunner2.jafkaServer.stopJafka();
		BeaverUtils.sleep(5 * 1000);
		brokerList = BeaverUtils.doGet(brokerListUrl);
		Assert.assertEquals(brokerList,"{\"error_code\":0,\"zkd\":[{\"ids\":\"1\",\"host\":\"beaver-shujie\",\"port\":9093}]}");
		System.out.println("Broker list = " + brokerList);

		jafkaRunner1.jafkaServer.stopJafka();
	}

	public static void main(String[] args) throws Exception {
		BrokerMonitorTest brokerMonitorTest = new BrokerMonitorTest();
		brokerMonitorTest.setUpServers();
		brokerMonitorTest.testBrokerMonitor();
		brokerMonitorTest.tearDownServers();
	}
}
