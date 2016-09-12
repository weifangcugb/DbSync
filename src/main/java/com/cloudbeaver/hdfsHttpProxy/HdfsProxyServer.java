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
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.client.dbbean.DatabaseBean;
import com.cloudbeaver.hdfsHttpProxy.proxybean.HdfsProxyServerConf;

public class HdfsProxyServer{
	private static Logger logger = Logger.getLogger(HdfsProxyServer.class);
	static String SERVER_PASSWORD = "123456";
	static HdfsProxyServerConf conf;
	public static long TOKEN_EXPIRE_INTERVAL = 300 * 1000;
	private static HdfsProxyServer hdfsProxyServer;
	private Server server;

	public void start(){
        server = new Server();

        HttpConfiguration httpConfig = new HttpConfiguration();
        httpConfig.setSecureScheme("https");
        httpConfig.setSecurePort(conf.getHttpsPort());
        httpConfig.setOutputBufferSize(10485760);
        httpConfig.setRequestHeaderSize(10485760);

        ServerConnector http_connector = new ServerConnector(server, new HttpConnectionFactory(httpConfig));
        http_connector.setPort(conf.getHttpPort());
        http_connector.setIdleTimeout(30000);

        SslContextFactory sslContextFactory = new SslContextFactory();
        sslContextFactory.setKeyStorePath(HdfsProxyServer.conf.getKeyStorePath());
        sslContextFactory.setKeyStorePassword(HdfsProxyServer.conf.getKeyStorePass());
        sslContextFactory.setKeyManagerPassword(HdfsProxyServer.conf.getKeyStorePass());

        HttpConfiguration https_config = new HttpConfiguration(httpConfig);
        https_config.addCustomizer(new SecureRequestCustomizer());

        ServerConnector https_connector = new ServerConnector(server, new SslConnectionFactory(sslContextFactory,"http/1.1"), new HttpConnectionFactory(https_config));
        https_connector.setPort(conf.getHttpsPort());
        https_connector.setIdleTimeout(500000);
        server.setConnectors(new Connector[]{ http_connector, https_connector });

        WebAppContext webApp = new WebAppContext();
        webApp.setContextPath("/");
        webApp.setResourceBase(".");
        webApp.addServlet(UploadFileServlet.class, "/uploadData");
        webApp.addServlet(GetFileInfoServlet.class, "/getFileInfo");
        webApp.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false");
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

	public static DatabaseBean getDataBaseBeanFromConf() {
		DatabaseBean dbBean = new DatabaseBean();
        dbBean.setDb(HdfsProxyServer.conf.getDbName());
        dbBean.setDbUserName(HdfsProxyServer.conf.getDbUser());
        dbBean.setDbPassword(HdfsProxyServer.conf.getDbPass());
        dbBean.setType(HdfsProxyServer.conf.getDbType());
        dbBean.setDbUrl(HdfsProxyServer.conf.getDbUrl());

        return dbBean;
	}

	public static String getSQLFromTableId(String tableId){
		return "select \"email\", \"password\", \"ownerId\",\"linkId\",\"uploadKey\" "
				+ "from \"Tables\" t, \"Files\" f, \"Users\" u "
				+ "where t.id = f.\"linkId\" and u.id = f.\"ownerId\" and t.id = '" + tableId + "';";
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
		HdfsProxyServer hServer = hdfsProxyServer;
		hServer.stop();
	}

    public static void startHdfsProxyServer(){
    	ApplicationContext appContext = new FileSystemXmlApplicationContext("conf/HdfsProxyServerConf.xml");
    	conf = appContext.getBean("HdfsProxyServerConf", HdfsProxyServerConf.class);

    	hdfsProxyServer = new HdfsProxyServer();
		hdfsProxyServer.start();
	}

	public static void main(String[] args){
		startHdfsProxyServer();
	}
}
