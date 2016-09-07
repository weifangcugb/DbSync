package com.cloudbeaver.hdfsHttpProxy;

import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.webapp.WebAppContext;
import org.junit.Assert;
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

        HttpConfiguration http_config = new HttpConfiguration();
        http_config.setSecureScheme("https");
        http_config.setSecurePort(conf.getHttps_port());
        http_config.setOutputBufferSize(10485760);
        http_config.setRequestHeaderSize(10485760);

        ServerConnector http_connector = new ServerConnector(server, new HttpConnectionFactory(http_config));
        http_connector.setPort(conf.getHttp_port());
        http_connector.setIdleTimeout(30000);

        SslContextFactory sslContextFactory = new SslContextFactory();
        sslContextFactory.setKeyStorePath("src/resources/https/keystore");
        sslContextFactory.setKeyStorePassword("OBF:19iy19j019j219j419j619j8");
        sslContextFactory.setKeyManagerPassword("OBF:19iy19j019j219j419j619j8");

        HttpConfiguration https_config = new HttpConfiguration(http_config);
        https_config.addCustomizer(new SecureRequestCustomizer());

        ServerConnector https_connector = new ServerConnector(server, new SslConnectionFactory(sslContextFactory,"http/1.1"), new HttpConnectionFactory(https_config));
        https_connector.setPort(conf.getHttps_port());
        https_connector.setIdleTimeout(500000);
        server.setConnectors(new Connector[]{ http_connector, https_connector });

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
    	logger.info("Http port = " + conf.getHttp_port());
    	logger.info("Https port = " + conf.getHttps_port());
    	logger.info("Buffer size = " + conf.getBufferSize());
    	Assert.assertNotNull("Http port is null", conf.getHttp_port());
    	Assert.assertNotNull("Https port is null", conf.getHttps_port());
    	Assert.assertNotNull("Buffer size is null", conf.getBufferSize());
    	Assert.assertTrue("Http port is less than 0", conf.getHttp_port() > 0);
    	Assert.assertTrue("Https port is less than 0", conf.getHttps_port() > 0);
    	Assert.assertTrue("Buffer size is less than 0", conf.getBufferSize() > 0);

		HdfsProxyServer hdfsProxyServer = new HdfsProxyServer();
		hdfsProxyServer.start();
	}

	public static void main(String[] args){
		startHdfsProxyServer();
	}
}
