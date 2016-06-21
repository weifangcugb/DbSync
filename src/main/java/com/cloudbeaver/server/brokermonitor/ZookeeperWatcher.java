package com.cloudbeaver.server.brokermonitor;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.cloudbeaver.server.brokermonitor.zkbean.BrokerListBean;
import com.cloudbeaver.server.brokermonitor.zkbean.BrokerZkNodeBean;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ZookeeperWatcher implements Runnable{
	public static final String _ERR1 = "KeeperErrorCode = ConnectionLoss for /brokers/ids";
	
	public static final int CLIENT_PORT = 2181;
	public static final String ZNODE = "/kafka/brokers/ids";
	private static ZooKeeper zk;

	private static BrokerListBean brokerList;

	Watcher wc;

	ObjectMapper mapper = new ObjectMapper();

	public String GetResult () {
        String Result = new String ();
        try {
        	Result = mapper.writeValueAsString(brokerList);
        } catch (JsonProcessingException e) {
        	e.printStackTrace();
        }
        return Result;
	}

	public void getBrokerList () {
		brokerList = new BrokerListBean(); // clear, set error_code to 0
		try {
			List<String> watched_nodes = zk.getChildren(ZNODE, true);
			if (watched_nodes.size() == 0) {
				brokerList.setError_code(2);
				return;
			}
			Iterator<String> it_wn = watched_nodes.iterator();
			while (it_wn.hasNext()) {
				String b_id = it_wn.next();
				String wn_path = ZNODE + "/" + b_id;
				String zkDataString = new String(zk.getData(wn_path, true, null));
				try {
					//String testJson = "{\"jmx_port\":-1,\"timestamp\":\"1466058881070\",\"endpoints\":[\"PLAINTEXT://localhost:9092\"],\"host\":\"localhost\",\"version\":3,\"port\":9093}\"";
					//Ob_res = mapper.readValue(testJson, ResType.class);
					BrokerZkNodeBean zkData = mapper.readValue(zkDataString, BrokerZkNodeBean.class);
					zkData.setIds(b_id);
					brokerList.addZkd(zkData);
				} catch (JsonParseException e) {
			         e.printStackTrace();
				} catch (JsonMappingException e) {
			         e.printStackTrace();
			    } catch (IOException e) {
			         e.printStackTrace();
			    }
			}
		} catch (Exception e) {
			//e.printStackTrace();
			if (_ERR1.equals(e.getMessage())) {
				brokerList.setError_code(1);
			}
		}
	}

	public ZookeeperWatcher () throws IOException {
		// host+port, session timeout, calback
		zk = new ZooKeeper("127.0.0.1:" + CLIENT_PORT, 500000,
			new Watcher() {
				public void process(WatchedEvent event) {}
			});
		// Initial information
		getBrokerList();
	}

	@Override
	public void run() {
		wc = new Watcher() {
			@Override
			public void process (WatchedEvent event) {
				getBrokerList();

				try {
					//zk.exists(ZNODE, wc);
					zk.getChildren(ZNODE, wc);
				} catch (KeeperException e1) {
					if (_ERR1.equals(e1.getMessage())) {
						brokerList.setError_code(1);
					}
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}
		};

		try {
			zk.getChildren(ZNODE, wc);
		} catch (KeeperException e1) {
			if (_ERR1.equals(e1.getMessage())) {
				brokerList.setError_code(1);
			}
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
	}
}
