package com.cloudbeaver.server.brokermonitor;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.server.brokermonitor.zkbean.BrokerListBean;
import com.cloudbeaver.server.brokermonitor.zkbean.BrokerZkNodeBean;
import com.fasterxml.jackson.databind.ObjectMapper;

public class BrokerMonitorServlet extends HttpServlet {
	private static Logger logger = Logger.getLogger(BrokerMonitorWebServer.class);

	public static final String _ERR1 = "KeeperErrorCode = ConnectionLoss for /kafka/brokers/ids";
	public static final String ZNODE = "/kafka/brokers/ids";

	public static final int ERROR_NO_KAFKA_NODE = 2;
	public static final int ZK_RETRY_TIMES = 3;
	public static final boolean USE_MONITOR_THREAD = true;
	public static final String ZK_CONN_STRING = "localhost:2181";

	private static volatile String brokerJsonString;

	public BrokerMonitorServlet() throws Exception {
        super();

        if (USE_MONITOR_THREAD) {
        	new ZKWatcher().start();
		}
	}

	@Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	for (int i = 0; i < ZK_RETRY_TIMES ; i++) {
    		ZooKeeper zkClient = null;
    		try {
    			if (!USE_MONITOR_THREAD) {
    				zkClient = new ZooKeeper(ZK_CONN_STRING, 50000, getEmptyWatcher());
    				brokerJsonString = getBrokerList(zkClient, false);
				}else if (brokerJsonString == null) {
					throw new ServletException("can't get brokers from zookeeper, wait zkWather thread start");
				}

    			PrintWriter pw = resp.getWriter();
    	    	pw.print(brokerJsonString);
    	        pw.flush();

    	        break;
    		} catch (KeeperException e) {
    			BeaverUtils.PrintStackTrace(e);
    			throw new IOException(e.getMessage());
    		} catch (InterruptedException e) {
    			if (i == ZK_RETRY_TIMES -1) {
					throw new IOException("retry many times, but allways interrupted, msg:" + e.getMessage());
				}
    		}finally{
    			if (zkClient != null) {
					try {
						zkClient.close();
					} catch (InterruptedException e) {
						BeaverUtils.printLogExceptionAndSleep(e, "close zkClient error, msg:", 1000);
					}
				}
    		}
		}
    }

	private Watcher getEmptyWatcher() {
		return new Watcher(){
			@Override
			public void process(WatchedEvent event) {}
		};
	}

	private class ZKWatcher extends Thread implements Watcher {
		ZooKeeper zkClient;

		private void initZKClient(){
			if (zkClient != null) {
				try {
					zkClient.close();
				} catch (InterruptedException e) {
					BeaverUtils.printLogExceptionAndSleep(e, "close zkClient error, msg:", 3 * 1000);
				}
			}

			while (true) {
				try {
					zkClient = new ZooKeeper(ZK_CONN_STRING, 50000, this);
					break;
				} catch (IOException e) {
					BeaverUtils.printLogExceptionAndSleep(e, "can't init zookeeper client, msg:", 3 * 1000);
				}
			}
		}

		private void getBrokerListFromZK(){
			while(true){
				try {
					brokerJsonString = getBrokerList(zkClient, true);
					break;
				} catch (KeeperException | InterruptedException | IOException e) {
					BeaverUtils.printLogExceptionAndSleep(e, "get brokerlist info error, msg:" + e.getMessage(), 3 * 1000);

					initZKClient();
				}
			}
		}

		@Override
		public void process (WatchedEvent event) {
			getBrokerListFromZK();
		}

		@Override
		public void run() {
			initZKClient();
			getBrokerListFromZK();
		}
	};

	public String getBrokerList(ZooKeeper zkClient, boolean watch) throws IOException, KeeperException, InterruptedException {
		List<String> brokerInfos = zkClient.getChildren(ZNODE, watch);

		ObjectMapper mapper = new ObjectMapper();
		BrokerListBean brokerList = new BrokerListBean();

		if (brokerInfos.size() == 0) {
			brokerList.setError_code(ERROR_NO_KAFKA_NODE);
		}else {
			Iterator<String> brokerInfo = brokerInfos.iterator();
			while (brokerInfo.hasNext()) {
				String b_id = brokerInfo.next();
				String wn_path = ZNODE + "/" + b_id;
				String zkDataString = new String(zkClient.getData(wn_path, true, null));

				//String testJson = "{\"jmx_port\":-1,\"timestamp\":\"1466058881070\",\"endpoints\":[\"PLAINTEXT://localhost:9092\"],\"host\":\"localhost\",\"version\":3,\"port\":9093}\"";
				//Ob_res = mapper.readValue(testJson, ResType.class);
				BrokerZkNodeBean zkData = mapper.readValue(getJsonFormat(zkDataString), BrokerZkNodeBean.class);
				zkData.setIds(b_id);
				brokerList.addZkd(zkData);
			}
		}

		return mapper.writeValueAsString(brokerList);
	}

	protected String getJsonFormat(String zkDataString) {
		return zkDataString;
	}
}
