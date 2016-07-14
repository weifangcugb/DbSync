package com.cloudbeaver.mockServer;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import com.cloudbeaver.client.common.BeaverUtils;
import com.sohu.jafka.Jafka;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class StandaloneJafkaServer {
	private static Logger logger = Logger.getLogger(StandaloneZKServer.class);
	private static Jafka broker = new Jafka();

	public void setUpJafkaServer(){
		Properties props = new Properties();
		props.setProperty("brokerid","1");
		props.setProperty("port","9092");
		props.setProperty("log.dir","/home/beaver/houjiajia/github/DBSync/jafkaLog");
		props.setProperty("enable.zookeeper","true");
		props.setProperty("zk.connect", "localhost:2181/kafka");
		props.setProperty("zk.connectiontimeout.ms", "600000");
		broker.start(props,null,null);
//		broker.awaitShutdown();
	}

	private void tearDownJafkaServer(){
		broker.close();
	}

	public static void main(String[] args) throws KeeperException, InterruptedException, IOException {
		StandaloneJafkaServer jafkaServer = new StandaloneJafkaServer();
		jafkaServer.setUpJafkaServer();
	}
}
