package com.cloudbeaver.dbsync;

import org.apache.http.client.HttpClient;
import org.apache.log4j.*;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.configuration2.*;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by gaobin on 16-4-6.
 */
public class DbSyncClient {

    private static Logger logger = Logger.getLogger(DbSyncClient.class);

    private HttpClient httpClient = null;

    private Map<String, String> conf;
    private Map<String, String> dbConf;
    private String taskJson;
    private Configurations configurations;
    private WatcherManager watcherManager;

    private String clientId;

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getTaskJson() {
		return taskJson;
	}

	public void setTaskJson(String taskJson) {
		this.taskJson = taskJson;
	}

    public void fetchTasks () {
        System.out.println(conf.get("tasks-server.url") + getClientId());
        //String json = new DbSyncClient().getTaskJson();
        String json = HttpClientHelper.get(conf.get("tasks-server.url") + clientId);
        setTaskJson(json);
        reloadTasks();
    }

    private void reloadTasks () {
        if (taskJson == null || taskJson.length() == 0) {
            return;
        }
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            watcherManager = objectMapper.readValue(taskJson, WatcherManager.class);
            watcherManager.setConf(dbConf);
        } catch (IOException e) {
            e.printStackTrace();
            logger.fatal("Create bean `watcherManager` failed : " + e.getMessage());
            System.exit(1);
        }
    }

    private void loadConfig () {
        conf = new HashMap<String, String>();
        dbConf = new HashMap<String, String>();
        configurations = new Configurations();
        try {
            Configuration configuration = configurations.properties("dbname.conf");
            Iterator<String> keys = configuration.getKeys();
            while (keys.hasNext()) {
                String key = keys.next();
                if (! key.startsWith("db.")) {
                    conf.put(key, configuration.getString(key));
                } else {
                    dbConf.put(key, configuration.getString(key));
                }
            }
        } catch (ConfigurationException e) {
            e.printStackTrace();
            logger.error("Read conf from `dbname.conf` failed : " + e.getMessage());
        }
    }

    @Deprecated
	public DbSyncClient() {
        // TODO: Get tasks from the server.
        taskJson = "{\"databases\":[{\"prison\":\"1\",\"db\":\"DocumentDB\",\"rowversion\":\"xgsj\",\"tables\":"
        		+ "[{\"table\":\"da_jbxx\"},{\"table\":\"da_jl\"},"
        		+ "{\"table\":\"da_qklj\"},{\"table\":\"da_shgx\"},"
        		+ "{\"table\":\"da_tzzb\"},{\"table\":\"da_tszb\",\"join\":[\"da_tscb\"],\"key\":\"da_tszb.bh=da_tscb.bh\"},"
        		+ "{\"table\":\"da_clzl\"},{\"table\":\"da_crj\"},{\"table\":\"da_swjd\"},"
        		+ "{\"table\":\"da_tc\"},{\"table\":\"da_zm\"},{\"table\":\"yzjc\"},{\"table\":\"xfzb\"},"
        		+ "{\"table\":\"djbd\"},{\"table\":\"db\"},{\"table\":\"nwfg\"},{\"table\":\"hjdd\"},"
        		+ "{\"table\":\"hjbd\"},{\"table\":\"jxcd\"},{\"table\":\"st\"},{\"table\":\"jy_rzfpbd\"},"
        		+ "{\"table\":\"jwbw\"},{\"table\":\"bwxb\"},{\"table\":\"tt\"},{\"table\":\"lbc\"},"
        		+ "{\"table\":\"ss\",\"join\":[\"ssfb\"],\"key\":\"ss.ssid=ssfb.ssid\"},"
        		+ "{\"table\":\"ks\",\"join\":[\"ksfb\"],\"key\":\"ks.bh=ksfb.bh AND ks.ksrq=ksfb.ksrq\"},"
        		+ "{\"table\":\"jfjjzb\"},{\"table\":\"tbjd\"},{\"table\":\"gzjs\"},{\"table\":\"qjfj\"},"
        		+ "{\"table\":\"yjdj\"},{\"table\":\"sg\"},{\"table\":\"em_zb\"},"
        		+ "{\"table\":\"em_qk\",\"join\":[\"em_zb\"],\"key\":\"em_qk.bh=em_zb.bh AND em_qk.pzrq=em_zb.pzrq\"},"
        		+ "{\"table\":\"em_jd\",\"join\":[\"em_zb\"],\"key\":\"em_jd.bh=em_zb.bh AND em_jd.pzrq=em_zb.pzrq\"},"
        		+ "{\"table\":\"em_jc\",\"join\":[\"em_zb\"],\"key\":\"em_jc.bh=em_zb.bh AND em_jc.pzrq=em_zb.pzrq\"},"
        		+ "{\"table\":\"em_sy\",\"join\":[\"em_zb\"],\"key\":\"em_sy.bh=em_zb.bh AND em_sy.pzrq=em_zb.pzrq\"},"
        		+ "{\"table\":\"fpa_zacy\",\"join\":[\"fpa_zb\",\"fpa_swry\"],\"key\":\"fpa_zacy.ah=fpa_zb.ah AND fpa_zacy.ah=fpa_swry.ah\"},"
        		+ "{\"table\":\"yma_zacy\",\"join\":[\"yma_zb\"],\"key\":\"yma_zacy.ah=yma_zb.ah\"},"
        		+ "{\"table\":\"wjp_bc\",\"join\":[\"wjp_zb\"],\"key\":\"wjp_bc.wjpid=wjp_zb.wjpid\"},"
        		+ "{\"table\":\"wyld_ry\",\"join\":[\"wyld_zb\"],\"key\":\"wyld_ry.wydid=wyld_zb.wydid\"},"
        		+ "{\"table\":\"hj\",\"join\":[\"hj_fb\"],\"key\":\"hj.hjid=hj_fb.hjid\"},{\"table\":\"khjf\"},"
        		+ "{\"table\":\"khjf_sd\"},{\"table\":\"khf\"},{\"table\":\"thdj\"},"
        		+ "{\"table\":\"wp_bgzb\",\"join\":[\"wp_bgbc\"],\"key\":\"wp_bgzb.bh=wp_bgbc.bh AND wp_bgzb.djrq=wp_bgbc.djrq\"},"
        		+ "{\"table\":\"wwzk\"},{\"table\":\"wwjc\"},{\"table\":\"wwbx\",\"join\":[\"wwzk\"],\"key\":\"wwbx.bh=wwzk.bh AND wwbx.pzrq=wwzk.pzrq\"},"
        		+ "{\"table\":\"sndd\"}]}]}";

    }

    public DbSyncClient (String clientId) {
        loadConfig();
        setClientId(clientId);
    }

    public String query() {
        assert (watcherManager != null);
        return watcherManager.query();
    }

    public void sendToFlume (String str) {
        str = str.replaceAll("\"", "\\\\\"");
        String flumeJson = "[{ \"headers\" : {}, \"body\" : \"" + str + "\" }]";
        // WRONG
//        HttpClientHelper.post(conf.get("flume-server.url"), flumeJson);
        // WRONG
//        Map<String, String> map = new HashMap<String, String>();
//        map.put("headers", "");
//        map.put("body", str);
//        HttpClientHelper.post(conf.get("flume-server.url"), map);
        String flumeUrl = conf.get("flume-server.url");
        if (flumeUrl != null && !flumeUrl.contains("://")) {
            flumeUrl = "http://" + flumeUrl;
        }
        try {
            URL url = new URL(flumeUrl);
            URLConnection connection = url.openConnection();
            connection.setDoOutput(true);
            PrintWriter pWriter = new PrintWriter((connection.getOutputStream()));
            logger.debug("Send message to flume-server : " + flumeJson);
            pWriter.write(flumeJson);
            pWriter.close();
            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line, result = "";
            while ((line = in.readLine()) != null) {
                result += line;
            }
            logger.debug("Got message from flume-server : " + result);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void queryAndSendToFlume () {
        sendToFlume(query());
    }

    protected void printConf() {
        // DEBUG
        for (String key : conf.keySet()) {
            System.out.println(key + " : " + conf.get(key));
        }
        for (String key : dbConf.keySet()) {
            System.out.println(key + " : " + dbConf.get(key));
        }
    }

    public static void main(String[] args) {

        if (args.length == 0) {
            System.out.println("Error: Client ID must be specified as a parameter.");
            args = new String[1];
            args[0] = "1";
        }
        System.out.println(args[0]);

        DbSyncClient dbSyncClient = new DbSyncClient(args[0]);
        dbSyncClient.setClientId("1");
        dbSyncClient.fetchTasks();

        while (true) {
            dbSyncClient.queryAndSendToFlume();
            try {
                Thread.sleep(1000 * 60 * 3);
            } catch (InterruptedException e) {
                e.printStackTrace();
                logger.debug("Sleep Interrupted !");
                break;
            }
        }

        // 下面都是测试用的。
        dbSyncClient.printConf();

        String brokerList = HttpClientHelper.get("http://br0:8088/bls");
        System.out.println(brokerList);

        // 405
        String brokerListPost = HttpClientHelper.post("http://bing.com/");
        System.out.println(brokerListPost);
        return;
    }

}
