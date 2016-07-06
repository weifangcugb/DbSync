package com.cloudbeaver.server.brokermonitor;

import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.servlet.ServletContextHandler;

import com.cloudbeaver.client.common.BeaverUtils;

public class BrokerMonitorWebServer {
	private static Logger logger = Logger.getLogger(BrokerMonitorWebServer.class);
	private static final int HTTP_PORT = 8028;

	private Server server;

	public void start(boolean shouldJoin){
		logger.info("starting brokerMonitorWebServer");

        server = new Server();

        ServerConnector connector = new ServerConnector(server);
        connector.setPort(HTTP_PORT);
        server.setConnectors(new Connector[] { connector });

        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        context.addServlet(BrokerMonitorServlet.class, "/getBrokerList");

        HandlerCollection handlers = new HandlerCollection();
        handlers.setHandlers(new Handler[] { context, new DefaultHandler() });
        server.setHandler(handlers);

        try {
			server.start();
			if (shouldJoin) {
				server.join();
			}
		} catch (Exception e) {
			BeaverUtils.PrintStackTrace(e);
			logger.fatal("mock server can't start");
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

	public static void startBrokerMonitor() {
		BrokerMonitorWebServer bmWebServer = new BrokerMonitorWebServer();
		bmWebServer.start(true);
	}

	public static void main(String[] args) {
		startBrokerMonitor();
	}
}
