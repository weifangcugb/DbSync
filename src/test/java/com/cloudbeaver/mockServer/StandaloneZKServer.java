package com.cloudbeaver.mockServer;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class StandaloneZKServer {
	private static Logger logger = Logger.getLogger(StandaloneZKServer.class);
	final ZooKeeperServerMain zkServer = new ZooKeeperServerMain();

	public void startZookeeper(){
		Properties props = new Properties();
        props.setProperty("tickTime", "2000");
        props.setProperty("dataDir", new File(System.getProperty("java.io.tmpdir"), "zookeeper").getAbsolutePath());
        props.setProperty("clientPort", "2181");
        props.setProperty("initLimit", "10");
        props.setProperty("syncLimit", "5");
        QuorumPeerConfig quorumConfig = new QuorumPeerConfig();
        try {
            quorumConfig.parseProperties(props);            
            final ServerConfig config = new ServerConfig();
            config.readFrom(quorumConfig);
            zkServer.runFromConfig(config);
        } catch(Exception e) {
        	logger.error("Start zookeeper server faile", e);  
            throw new RuntimeException(e);
        }
	}

	public static void main(String[] args) {
		StandaloneZKServer zkServer = new StandaloneZKServer();
		zkServer.startZookeeper();
	}

}
