package com.cloudbeaver.hdfsHttpProxy;

import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.servlet.ServletContextHandler;
import com.cloudbeaver.client.common.BeaverUtils;

public class HdfsProxyServer{
	private static Logger logger = Logger.getLogger(HdfsProxyServer.class);
	private static final int HTTP_PORT = 8811;
	private Server server;

    public void start(){
        server = new Server();

        ServerConnector connector = new ServerConnector(server);
        connector.setPort(HTTP_PORT);
        server.setConnectors(new Connector[] { connector });

        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        context.addServlet(HdfsProxyServlet.class, "/uploaddata");
        context.addServlet(GetFileInfo.class, "/fileinfo");

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
		HdfsProxyServer hdfsProxyServer = new HdfsProxyServer();
		hdfsProxyServer.start();
	}

	public static void main(String[] args){
		startHdfsProxyServer();
	}
}
