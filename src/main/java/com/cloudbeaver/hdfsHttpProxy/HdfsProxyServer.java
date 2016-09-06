package com.cloudbeaver.hdfsHttpProxy;

import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.webapp.WebAppContext;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.hdfsHttpProxy.proxybean.HdfsProxyServerConf;

public class HdfsProxyServer{
	private static Logger logger = Logger.getLogger(HdfsProxyServer.class);
	private static HdfsProxyServerConf conf;
	private Server server;

    public static HdfsProxyServerConf getConf() {
		return conf;
	}

	public static void setConf(HdfsProxyServerConf conf) {
		HdfsProxyServer.conf = conf;
	}

	public void start(){
        server = new Server();

        HttpConfiguration https_config = new HttpConfiguration();
        https_config.setSecureScheme("https");
        https_config.setSecurePort(conf.getPort());
        https_config.setOutputBufferSize(10485760);
        https_config.setRequestHeaderSize(10485760);
        https_config.addCustomizer(new SecureRequestCustomizer());

        SslContextFactory sslContextFactory = new SslContextFactory();
        sslContextFactory.setKeyStorePath("src/resources/https/keystore");
        sslContextFactory.setKeyStorePassword("OBF:19iy19j019j219j419j619j8");
        sslContextFactory.setKeyManagerPassword("OBF:19iy19j019j219j419j619j8");

        ServerConnector connector = new ServerConnector(server, new SslConnectionFactory(sslContextFactory,"http/1.1"), new HttpConnectionFactory(https_config));
        connector.setPort(conf.getPort());
        connector.setIdleTimeout(500000);
        server.setConnectors(new Connector[]{ connector });

        WebAppContext webApp = new WebAppContext();
        webApp.setContextPath("/");
        webApp.setResourceBase(".");
        webApp.addServlet(HdfsProxyServlet.class, "/uploadData");
        webApp.addServlet(GetFileInfoServlet.class, "/getFileInfo");
        webApp.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "true");
        webApp.setInitParameter("org.eclipse.jetty.servlet.Default.useFileMappedBuffer", "false");
        server.setHandler(webApp);

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
    	conf = appContext.getBean("HdfsProxyConf", HdfsProxyServerConf.class);

		HdfsProxyServer hdfsProxyServer = new HdfsProxyServer();
		hdfsProxyServer.start();
	}

	public static void main(String[] args){
		startHdfsProxyServer();
	}
}
