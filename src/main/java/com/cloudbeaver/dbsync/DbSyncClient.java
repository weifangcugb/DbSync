package com.cloudbeaver.dbsync;

import org.apache.http.client.HttpClient;
import org.apache.log4j.*;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.configuration2.*;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by gaobin on 16-4-6.
 */
public class DbSyncClient {

    private final String DB_SYNC_CLIENT_CONFIG_FILE_NAME = "DbSyncClient.conf";
    private final String TASKS_SERVER_URL = "tasks-server.url";
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
        System.out.println(conf.get(TASKS_SERVER_URL) + getClientId());
        String json = HttpClientHelper.get(conf.get(TASKS_SERVER_URL) + clientId);
        logger.debug("Tasks : " + json);
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
            Configuration configuration
                    = configurations.properties(DB_SYNC_CLIENT_CONFIG_FILE_NAME);
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
            logger.error("Read conf from `" + DB_SYNC_CLIENT_CONFIG_FILE_NAME + "` failed : "
                    + e.getMessage());
        }
    }

	private String defaultTaskJson() {
        return  "{\"databases\":[{\"prison\":\"1\",\"db\":\"DocumentDB\",\"rowversion\":\"xgsj\",\"tables\":"
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

    public DbSyncClient () {
        loadConfig();
        if (conf.containsKey("client.id")) {
            setClientId(conf.get("client.id"));
        } else {
            setClientId("1");
        }
    }

    public String query() {
        //assert (watcherManager != null);
        if (watcherManager == null)
            return "";
        return watcherManager.query();
    }

    public void sendToFlume (String str) {
        str = str.replaceAll("\"", "\\\\\"");
        String flumeJson = "[{ \"headers\" : {}, \"body\" : \"" + str + "\" }]";
        HttpClientHelper.post(conf.get("flume-server.url"), flumeJson);
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

        DbSyncClient dbSyncClient = new DbSyncClient();
        dbSyncClient.fetchTasks();

        while (true) {
            dbSyncClient.queryAndSendToFlume();
            try {
                // Thread.sleep(1000 * 3); // DEBUG QUICKLY
                Thread.sleep(1000 * 60 * 3); // PRODUCT
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
