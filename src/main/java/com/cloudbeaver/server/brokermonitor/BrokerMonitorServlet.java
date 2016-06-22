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
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class BrokerMonitorServlet extends HttpServlet {
	private static Logger logger = Logger.getLogger(BrokerMonitorWebServer.class);

	public static final String _ERR1 = "KeeperErrorCode = ConnectionLoss for /kafka/brokers/ids";
	public static final String ZNODE = "/kafka/brokers/ids";

	public static final int ERROR_NO_KAFKA_NODE = 2;
	public static final int ZK_RETRY_TIMES = 3;
	public static final boolean USE_MONITOR_THREAD = true;
	public static final String ZK_CONN_STRING = "localhost:2181";

	private static String brokerJsonString;

	public BrokerMonitorServlet() throws Exception {
        super();

        if (USE_MONITOR_THREAD) {
        	startZKMonitorThread();
		}
	}

	@Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	for (int i = 0; i < ZK_RETRY_TIMES ; i++) {
    		try {
    			if (!USE_MONITOR_THREAD) {
    				brokerJsonString = getBrokerList(null, false);
				}

    			PrintWriter pw = resp.getWriter();
    	    	pw.print(brokerJsonString);
    	        pw.flush();
    		} catch (KeeperException e) {
    			BeaverUtils.PrintStackTrace(e);
    			throw new IOException(e.getMessage());
    		} catch (InterruptedException e) {
    			if (i == ZK_RETRY_TIMES -1) {
					throw new IOException("retry many times, but allways interrupted, msg:" + e.getMessage());
				}
    		}
		}
    }

    private void startZKMonitorThread() {
        Thread monitorThread = new Thread(new Runnable() {
			@Override
			public void run() {
			    Watcher wc = new Watcher() {
					@Override
					public void process (WatchedEvent event) {
						getBrokerListInWather(this);
					}
				};

				getBrokerListInWather(wc);
			}
		});

        monitorThread.start();
	}

    public void getBrokerListInWather(Watcher watcher){
		try {
			brokerJsonString = getBrokerList(watcher, true);
		} catch (IOException | KeeperException | InterruptedException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("broker watcher added failed, msg:" + e.getMessage());
		}
    }

	public String getBrokerList(Watcher watcher, boolean watch) throws IOException, KeeperException, InterruptedException {
		ZooKeeper zk = null;
		try {
			zk = new ZooKeeper(ZK_CONN_STRING, 500000, watcher);
			List<String> brokerIds = zk.getChildren(BrokerMonitorServlet.ZK_CONN_STRING, watch);
			return getBrokerListJson(brokerIds, zk);
		} finally{
			if (zk != null) {
				try {
					zk.close();
				} catch (InterruptedException e) {
				}
			}
		}
	}

	private String getBrokerListJson(List<String> brokerLists, ZooKeeper zk) throws KeeperException, InterruptedException, JsonParseException, JsonMappingException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		BrokerListBean brokerList = new BrokerListBean();

		if (brokerLists.size() == 0) {
			brokerList.setError_code(ERROR_NO_KAFKA_NODE);
		}else {
			Iterator<String> it_wn = brokerLists.iterator();
			while (it_wn.hasNext()) {
				String b_id = it_wn.next();
				String wn_path = ZNODE + "/" + b_id;
				String zkDataString = new String(zk.getData(wn_path, true, null));

				//String testJson = "{\"jmx_port\":-1,\"timestamp\":\"1466058881070\",\"endpoints\":[\"PLAINTEXT://localhost:9092\"],\"host\":\"localhost\",\"version\":3,\"port\":9093}\"";
				//Ob_res = mapper.readValue(testJson, ResType.class);
				BrokerZkNodeBean zkData = mapper.readValue(zkDataString, BrokerZkNodeBean.class);
				zkData.setIds(b_id);
				brokerList.addZkd(zkData);
			}
		}

		return mapper.writeValueAsString(brokerList);
	}
}
