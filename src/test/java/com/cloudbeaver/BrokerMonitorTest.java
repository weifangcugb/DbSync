package com.cloudbeaver;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.mockServer.BrokerMonitorServletTest;
import com.cloudbeaver.mockServer.MockWebServer;
import com.cloudbeaver.mockServer.StandaloneJafkaServer;
import com.cloudbeaver.mockServer.StandaloneZKServer;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.cloudbeaver.server.brokermonitor.zkbean.BrokerListBean;
import com.cloudbeaver.server.brokermonitor.zkbean.BrokerZkNodeBean;;

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
	public void testBrokerMonitor() throws IOException, InterruptedException {
		ZookeeperRunner zkRunner = new ZookeeperRunner();
		zkRunner.start();
		BeaverUtils.sleep(5 * 1000);

		JafkaRunner jafkaRunner1 = new JafkaRunner();
		jafkaRunner1.start();
		BeaverUtils.sleep(5 * 1000);
		String brokerList = BeaverUtils.doGet(brokerListUrl);
		System.out.println("Broker list = " + BeaverUtils.doGet(brokerListUrl));
		checkBrokerList(brokerList, 1);

		JafkaRunner jafkaRunner2 = new JafkaRunner();
		jafkaRunner2.start();
		BeaverUtils.sleep(5 * 1000);
		brokerList = BeaverUtils.doGet(brokerListUrl);
		System.out.println("Broker list = " + brokerList);
		checkBrokerList(brokerList, 2);

		jafkaRunner2.jafkaServer.stopJafka();
		BeaverUtils.sleep(5 * 1000);
		brokerList = BeaverUtils.doGet(brokerListUrl);
		System.out.println("Broker list = " + brokerList);
		checkBrokerList(brokerList, 1);

		JafkaRunner jafkaRunner3 = new JafkaRunner();
		jafkaRunner3.start();
		BeaverUtils.sleep(5 * 1000);
		brokerList = BeaverUtils.doGet(brokerListUrl);
		System.out.println("Broker list = " + brokerList);
		checkBrokerList(brokerList, 2);

		jafkaRunner3.jafkaServer.stopJafka();
		BeaverUtils.sleep(5 * 1000);
		brokerList = BeaverUtils.doGet(brokerListUrl);
		System.out.println("Broker list = " + brokerList);
		checkBrokerList(brokerList, 1);

		jafkaRunner1.jafkaServer.stopJafka();
		BeaverUtils.sleep(5 * 1000);
		brokerList = BeaverUtils.doGet(brokerListUrl);
		System.out.println("Broker list = " + brokerList);
		checkBrokerList(brokerList, 0);
	}

	@Test
	public void testBrokerMonitor2() throws IOException, InterruptedException {
		BrokerMonitorServletTest.setUseMonitorThread(true);
		BrokerMonitorServletTest.setZkConnString("localhost:" + (StandaloneZKServer.port + 1));
		testBrokerMonitor();
	}

	public void checkBrokerList(String brokerList, int brokerNums) throws JsonParseException, JsonMappingException, IOException {
		ObjectMapper oMapper = new ObjectMapper();
		BrokerListBean brokerListBean = oMapper.readValue(brokerList, BrokerListBean.class);
		int error_code = brokerListBean.getError_code();
		ArrayList<BrokerZkNodeBean> zkd = brokerListBean.getZkd();
		Assert.assertEquals(brokerNums, zkd.size());
		if(zkd.size() > 0){
			Assert.assertEquals(0, error_code);
		}
		else{
			Assert.assertEquals(2, error_code);
		}
		for(int i = 0; i < zkd.size(); i++){
			BrokerZkNodeBean brokerZkNodeBean = zkd.get(i);
			String ids = brokerZkNodeBean.getIds();
			int port = brokerZkNodeBean.getPort();
			Assert.assertNotNull("ids is null", ids);
			Assert.assertNotNull("port is null", port);
			Assert.assertTrue("Port doesn't match ids!", Integer.parseInt(ids) == port-9092);
		}
	}

	public static void main(String[] args) throws Exception {
		BrokerMonitorTest brokerMonitorTest = new BrokerMonitorTest();
		brokerMonitorTest.setUpServers();
		brokerMonitorTest.testBrokerMonitor();
		brokerMonitorTest.tearDownServers();

		brokerMonitorTest.setUpServers();
		brokerMonitorTest.testBrokerMonitor2();
		brokerMonitorTest.tearDownServers();
	}
}
