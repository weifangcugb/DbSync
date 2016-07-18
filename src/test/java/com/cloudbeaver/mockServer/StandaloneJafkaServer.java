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
	private static String JAFKA_LOG_PATH_PREFIX = "/home/beaver/houjiajia/github/DBSync/jafkaLog/";

	public void setUpJafkaServer(Jafka broker, String brokerId, String port, String logPath){
		Properties props = new Properties();
		props.setProperty("brokerid",brokerId);
		props.setProperty("port",port);
		props.setProperty("log.dir",JAFKA_LOG_PATH_PREFIX+logPath);
		props.setProperty("enable.zookeeper","true");
		props.setProperty("zk.connect", "localhost:2181/kafka");
		props.setProperty("zk.connectiontimeout.ms", "6000000");
		broker.start(props,null,null);
		broker.awaitShutdown();
	}

	public void tearDownJafkaServer(Jafka broker) {
		broker.close();
	}

	public static void main(String[] args) throws KeeperException, InterruptedException, IOException {
		StandaloneJafkaServer jafkaServer = new StandaloneJafkaServer();
		jafkaServer.setUpJafkaServer(new Jafka(),"1","9092","log1");
	}
}
