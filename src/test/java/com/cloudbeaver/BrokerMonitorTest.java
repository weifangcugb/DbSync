package com.cloudbeaver;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.mockServer.BrokerMonitorServletTest;
import com.cloudbeaver.mockServer.MockWebServer;
import com.cloudbeaver.mockServer.StandaloneJafkaServer;
import com.cloudbeaver.mockServer.StandaloneZKServer;
import com.sohu.jafka.Jafka;

import scala.collection.parallel.ParIterableLike.Forall;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class BrokerMonitorTest implements Callable<Object>{
	private static MockWebServer mockServer = new MockWebServer();
	private boolean HAS_ZK_BEEN_SETUP;
	private boolean GET_BROKERJSON;
	private static int id = 1;
	private static int port = 9092;
	private static String logPath = "log";
	private Jafka broker = new Jafka();
	private static String url = "http://localhost:8877/getBrokerList";

	public BrokerMonitorTest(boolean hAS_ZK_BEEN_SETUP, boolean gET_BROKERJSON) throws Exception {
		super();
		HAS_ZK_BEEN_SETUP = hAS_ZK_BEEN_SETUP;
		GET_BROKERJSON = gET_BROKERJSON;
	}

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
    public void testBrokerMonitor() throws Exception{		
		final ExecutorService service = Executors.newFixedThreadPool(4);
		BrokerMonitorTest taskThread1 = new BrokerMonitorTest(false, false);
        service.submit(taskThread1);

        BrokerMonitorTest taskThread2 = new BrokerMonitorTest(true, false);
        service.submit(taskThread2);

        BrokerMonitorTest taskThread3 = new BrokerMonitorTest(true, false);
        Future<Object> taskFuture3 = service.submit(taskThread3);

        BrokerMonitorTest taskThread4 = new BrokerMonitorTest(false, true);
        service.submit(taskThread4);
        try {
            taskFuture3.get(10000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            System.out.println("Timeout, cancle broker 2");
            taskFuture3.cancel(true);
            StandaloneJafkaServer jafkaServer = new StandaloneJafkaServer();
            jafkaServer.tearDownJafkaServer(taskThread3.broker);
            System.out.println("shutdown broker 2");
        }
    }

	public static void main(String[] args) throws Exception {
		BrokerMonitorTest brokerMonitorTest = new BrokerMonitorTest(false, false);
		brokerMonitorTest.testBrokerMonitor();
	}

	@Override
	public Object call() throws Exception {
		System.out.println("HAS_ZK_BEEN_SETUP = " + HAS_ZK_BEEN_SETUP);
		if(!HAS_ZK_BEEN_SETUP && !GET_BROKERJSON){
			System.out.println("Start Zookeeper!");
			StandaloneZKServer zkServer = new StandaloneZKServer();
			zkServer.setUpZookeeperServer();
		}
		else if(HAS_ZK_BEEN_SETUP && !GET_BROKERJSON){
			System.out.println("Start Jafka!");
			StandaloneJafkaServer jafkaServer = new StandaloneJafkaServer();
			System.out.println("id = " + id + ", port = " + port);
			jafkaServer.setUpJafkaServer(broker, String.valueOf(id), String.valueOf(port++), logPath+id++);
		}
		for(int i = 0 ; i < 10 ; i++){
			System.out.println("Broker list = " + BeaverUtils.doGet(url));
			Thread.sleep(2000);
		}
		return null;
	}

}
