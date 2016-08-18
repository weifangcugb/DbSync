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

public class FileServer{
	private static Logger logger = Logger.getLogger(FileServer.class);
	private static final int HTTP_PORT = 8090;

    public void start(){
        Server server = new Server();

        ServerConnector connector = new ServerConnector(server);
        connector.setPort(HTTP_PORT);
        server.setConnectors(new Connector[] { connector });

        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        context.addServlet(GetFileDataServer.class, "/filedata");

        HandlerCollection handlers = new HandlerCollection();
        handlers.setHandlers(new Handler[] { context, new DefaultHandler() });
        server.setHandler(handlers);
        
 
        try {
        	server.start();
            server.join();
		} catch (Exception e) {
			BeaverUtils.PrintStackTrace(e);
			logger.fatal("mock server can't start");
		}
    }

    public static void startFileServer() throws Exception{
		FileServer fileServer = new FileServer();
		fileServer.start();
	}

	public static void main(String[] args) throws Exception{
		startFileServer();
	}
}
