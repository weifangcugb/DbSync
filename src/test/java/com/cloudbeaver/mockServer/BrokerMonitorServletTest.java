package com.cloudbeaver.mockServer;

import java.util.HashMap;
import java.util.Map;

import com.cloudbeaver.server.brokermonitor.BrokerMonitorServlet;

import net.sf.json.JSONObject;

public class BrokerMonitorServletTest extends BrokerMonitorServlet{

	public BrokerMonitorServletTest() throws Exception {
		super();
		ZNODE = "/brokers/ids";
		_ERR1 = "KeeperErrorCode = ConnectionLoss for " + ZNODE;
		USE_MONITOR_THREAD = false;
	}

	public static void setUseMonitorThread(boolean use_monitor_thread) {
		USE_MONITOR_THREAD = use_monitor_thread;
	}

	public static void setZkConnString(String zkConnString) {
		ZK_CONN_STRING = zkConnString;
	}

	protected String getJsonFormat(String zkDataString) {
		//kafka:{"jmx_port":-1,"timestamp":"1468478612324","host":"localhost","version":1,"port":9092}
		//jafka:beaver-inter1-1468478831294:beaver-inter1:9092
		String []strs = zkDataString.split(":");
		Map<String, String> map = new HashMap<String, String>();
		map.put("host", strs[1]);
		map.put("port", strs[2]);
		JSONObject jsonObject = JSONObject.fromObject(map);
		return jsonObject.toString();
	}

	public static void main(String []args) throws Exception {
		BrokerMonitorServletTest brokerMonitorServletTest = new BrokerMonitorServletTest();
		brokerMonitorServletTest.getJsonFormat("beaver-inter1-1468478831294:beaver-inter1:9092");
	}
}
