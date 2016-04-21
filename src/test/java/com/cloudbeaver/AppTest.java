package com.cloudbeaver;

import net.sf.json.JSONArray;

import org.junit.Test;

import com.cloudbeaver.client.dbUploader.DbUploader;
import com.cloudbeaver.client.dbbean.DatabaseBean;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import junit.framework.TestCase;

public class AppTest extends TestCase {
	private String taskJson = "{\"databases\":[{\"prison\":\"1\",\"db\":\"DocumentDB\",\"rowversion\":\"xgsj\",\"tables\":"
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

	@Test
	public void testGetMsg() throws Exception {
		DbUploader dbUploader = new DbUploader();
		dbUploader.beforeTask();

		for (int index = 0; index < dbUploader.getThreadNum(); index++) {
			DatabaseBean dbBean = (DatabaseBean) dbUploader.getTaskObject(index);
			if (dbBean == null) {
				continue;
			}

			String reply = dbUploader.getDbUploadData(dbBean);
//			reply:[{"hdfs_prison":"1","hdfs_db":"DocumentDB", xxx}]
			System.out.println("reply:" + reply);

			ObjectMapper oMapper = new ObjectMapper();
			JsonNode root = oMapper.readTree(reply);
			for (int i = 0; i < root.size(); i++) {
				JsonNode item = root.get(i);
				assertEquals(item.get("hdfs_prison").asInt(), 1);
				assertEquals(item.get("hdfs_db").asText(), "DocumentDB");
			}
		}
	}

	@Test
	public void testJsonArrayEmpty(){
		JSONArray jArray = new JSONArray();
		assertEquals(jArray.toString(), "[]");
	}
}
