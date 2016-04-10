package com.cloudbeaver.dbsync;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.impl.client.*;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.HttpContext;
import org.apache.log4j.*;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.configuration2.*;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.http.client.*;

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

    public String getTaskJson() {
		return taskJson;
	}

	public void setTaskJson(String taskJson) {
		this.taskJson = taskJson;
	}

	public DbSyncClient() {
        conf = new HashMap<String, String>();
        dbConf = new HashMap<String, String>();
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

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            watcherManager = objectMapper.readValue(taskJson, WatcherManager.class);
            watcherManager.setConf(dbConf);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Create bean `watcherManager` failed : " + e.getMessage());
        }

    }

    public String query() {
        return watcherManager.query();
    }

    private void printConf() {
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

        dbSyncClient.printConf();
        System.out.println(dbSyncClient.query());

        String brokerList = HttpClientHelper.get("http://br0:8088/bls");
        System.out.println(brokerList);

        // 405
        String brokerListPost = HttpClientHelper.post("http://bing.com/");
        System.out.println(brokerListPost);
        return;
    }

}
