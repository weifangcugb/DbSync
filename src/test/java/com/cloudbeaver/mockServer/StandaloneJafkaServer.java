package com.cloudbeaver.mockServer;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import com.sohu.jafka.Jafka;
import java.io.IOException;
import java.util.Properties;

public class StandaloneJafkaServer {
	private static Logger logger = Logger.getLogger(StandaloneJafkaServer.class);
	private static int brokerId = 0;
	private static String JAFKA_LOG_PATH_PREFIX = "logs/";

	Jafka broker = new Jafka();

	public void startJafka(){
		int port = 9092 + (++brokerId);
		Properties props = new Properties();
		props.setProperty("brokerid",brokerId + "");
		props.setProperty("port", port + "");
		props.setProperty("log.dir",JAFKA_LOG_PATH_PREFIX);
		props.setProperty("enable.zookeeper","true");
		props.setProperty("zk.connect", "localhost:" + StandaloneZKServer.port);
		props.setProperty("zk.connectiontimeout.ms", "6000000");

		broker.start(props,null,null);
		logger.info("Jafka start now, id:" + brokerId + " port:" + port);

		broker.awaitShutdown();
	}

	public void stopJafka() {
		broker.close();
	}

	public static void main(String[] args) throws KeeperException, InterruptedException, IOException {
		StandaloneJafkaServer jafkaServer = new StandaloneJafkaServer();
		jafkaServer.startJafka();
	}
}
