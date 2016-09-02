package com.cloudbeaver.hdfsHttpProxy.proxybean;

import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.cloudbeaver.client.common.BeaverUtils;

public class HdfsProxyServer{
	private static Logger logger = Logger.getLogger(HdfsProxyServer.class);
	private static HdfsProxyConf conf;
	private Server server;

    public static HdfsProxyConf getConf() {
		return conf;
	}

	public static void setConf(HdfsProxyConf conf) {
		HdfsProxyServer.conf = conf;
	}

	public void start(){
        server = new Server();

        ServerConnector connector = new ServerConnector(server);
        connector.setPort(conf.getPort());
        server.setConnectors(new Connector[] { connector });

        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        context.addServlet(HdfsProxyServlet.class, "/uploaddata");
        context.addServlet(GetFileInfoServlet.class, "/fileinfo");

        HandlerCollection handlers = new HandlerCollection();
        handlers.setHandlers(new Handler[] { context, new DefaultHandler() });
        server.setHandler(handlers);

        try {
        	server.start();
            server.join();
		} catch (Exception e) {
			BeaverUtils.PrintStackTrace(e);
			logger.fatal("hdfs proxy server can't start");
		}
    }

    public void stop(){
		if (server != null) {
			try {
				server.stop();
			} catch (Exception e) {
				BeaverUtils.PrintStackTrace(e);
				logger.error("mock server can't stop");
			}
		}
	}

    public static void stopHdfsProxyServer(){
		HdfsProxyServer hdfsProxyServer = new HdfsProxyServer();
		hdfsProxyServer.stop();
	}

    public static void startHdfsProxyServer(){
    	ApplicationContext appContext = new FileSystemXmlApplicationContext("conf/HdfsProxyConf.xml");
    	conf = appContext.getBean("HdfsProxyConf", HdfsProxyConf.class);

		HdfsProxyServer hdfsProxyServer = new HdfsProxyServer();
		hdfsProxyServer.start();
	}

	public static void main(String[] args){
		startHdfsProxyServer();
	}
}
