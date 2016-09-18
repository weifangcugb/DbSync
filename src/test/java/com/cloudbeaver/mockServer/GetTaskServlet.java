package com.cloudbeaver.mockServer;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.client.dbbean.DatabaseBean;
import com.cloudbeaver.client.dbbean.MultiDatabaseBean;
import com.cloudbeaver.client.dbbean.TableBean;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@WebServlet("/api/business/sync/*")
public class GetTaskServlet extends HttpServlet{
	private static Logger logger = Logger.getLogger(GetTaskServlet.class);
	private static String getTaskApi = "/api/business/sync/";
	private static MultiDatabaseBean databaseBeans;
	private static String clientId = null;
	private static String PROJECT_ABSOLUTE_PATH = System.getProperty("user.dir");
	public static long now = System.currentTimeMillis();
	public static long fiveDayBefore = (now - now % (24 * 3600 * 1000))- 24 * 3600 * 1000 * 5 - 8 * 3600 * 1000;
	public static long fourDayBefore = (now - now % (24 * 3600 * 1000))- 24 * 3600 * 1000 * 4 - 8 * 3600 * 1000;
	public static String fourDayBeforeString = BeaverUtils.timestampToDateString(fourDayBefore);
	public static Map<String, String> map = new HashMap<String, String>();

	{
		map.put("DocumentDB", "sqlserver");
		map.put("MeetingDB", "webservice");
		map.put("TalkDB", "webservice");
		map.put("PrasDB", "webservice");
		map.put("JfkhDB", "oracle");
		map.put("DocumentDBForSqlite", "sqlite");
		map.put("DocumentFiles", "file");
		map.put("VideoMeetingDB", "sqlserver");
		map.put("HelpDB", "sqlserver");
	}

//	public static String documentDBInitJson = "{\"databases\":[{\"db\":\"DocumentDB\",\"rowversion\":\"xgsj2\",\"tables\":"
//			+ "[{\"table\":\"da_jbxx\",\"xgsj\":\"0000000000000000\"},{\"table\":\"da_jl\",\"xgsj\":\"0000000000000000\"},"
//			+ "{\"table\":\"da_qklj\",\"xgsj\":\"0000000000000000\"},{\"table\":\"da_shgx\",\"xgsj\":\"0000000000000000\"},"
//			+ "{\"table\":\"da_tzzb\",\"xgsj\":\"0000000000000000\"},{\"table\":\"da_tszb\",\"join\":[\"da_tsbc\"],\"key\":\"da_tszb.bh=da_tsbc.bh\",\"xgsj\":\"0000000000000000\"},"
//			+ "{\"table\":\"da_clzl\",\"xgsj\":\"0000000000000000\"},{\"table\":\"da_crj\",\"xgsj\":\"0000000000000000\"},"
//			+ "{\"table\":\"da_swdj\",\"xgsj\":\"0000000000000000\"},{\"table\":\"da_tc\",\"xgsj\":\"0000000000000000\"},"
//			+ "{\"table\":\"da_zm\",\"xgsj\":\"0000000000000000\"},{\"table\":\"yzjc\",\"xgsj\":\"0000000000000000\"},"
//			+ "{\"table\":\"xfzb\",\"xgsj\":\"0000000000000000\"},{\"table\":\"djbd\",\"xgsj\":\"0000000000000000\"},"
//			+ "{\"table\":\"db\",\"xgsj\":\"0000000000000000\"},{\"table\":\"nwfg\",\"xgsj\":\"0000000000000000\"},{\"table\":\"hjdd\",\"xgsj\":\"0000000000000000\"},"
//			+ "{\"table\":\"hjbd\",\"xgsj\":\"0000000000000000\"},{\"table\":\"jxcd\",\"xgsj\":\"0000000000000000\"},{\"table\":\"st\",\"xgsj\":\"0000000000000000\"},"
//			+ "{\"table\":\"jy_rzfpbd\",\"xgsj\":\"0000000000000000\"},{\"table\":\"jwbw\",\"xgsj\":\"0000000000000000\"},"
//			+ "{\"table\":\"bwxb\",\"xgsj\":\"0000000000000000\"},{\"table\":\"tt\",\"xgsj\":\"0000000000000000\"},{\"table\":\"lbc\",\"xgsj\":\"0000000000000000\"},"
//			+ "{\"table\":\"ss\",\"join\":[\"ssfb\"],\"key\":\"ss.ssid=ssfb.ssid\",\"xgsj\":\"0000000000000000\"},"
//			+ "{\"table\":\"ks\",\"join\":[\"ksfb\"],\"key\":\"ks.bh=ksfb.bh AND ks.ksrq=ksfb.ksrq\",\"xgsj\":\"0000000000000000\"},"
//			+ "{\"table\":\"jfjjzb\",\"xgsj\":\"0000000000000000\"},{\"table\":\"tbjd\",\"xgsj\":\"0000000000000000\"},{\"table\":\"gzjs\",\"xgsj\":\"0000000000000000\"},"
//			+ "{\"table\":\"qjfj\",\"xgsj\":\"0000000000000000\"},{\"table\":\"yjdj\",\"xgsj\":\"0000000000000000\"},{\"table\":\"sg\",\"xgsj\":\"0000000000000000\"},"
//			+ "{\"table\":\"em_zb\",\"xgsj\":\"0000000000000000\"},{\"table\":\"em_qk\",\"join\":[\"em_zb\"],\"key\":\"em_qk.bh=em_zb.bh\",\"xgsj\":\"0000000000000000\"},"
//			+ "{\"table\":\"em_jd\",\"join\":[\"em_zb\"],\"key\":\"em_jd.bh=em_zb.bh\",\"xgsj\":\"0000000000000000\"},"
//			+ "{\"table\":\"em_jc\",\"join\":[\"em_zb\"],\"key\":\"em_jc.bh=em_zb.bh\",\"xgsj\":\"0000000000000000\"},"
//			+ "{\"table\":\"em_sy\",\"join\":[\"em_zb\"],\"key\":\"em_sy.bh=em_zb.bh\",\"xgsj\":\"0000000000000000\"},"
//			+ "{\"table\":\"fpa_zacy\",\"join\":[\"fpa_zb\",\"fpa_swry\"],\"key\":\"fpa_zacy.ah=fpa_zb.ah AND fpa_zacy.ah=fpa_swry.ah\",\"xgsj\":\"0000000000000000\"},"
//			+ "{\"table\":\"yma_zacy\",\"join\":[\"yma_zb\"],\"key\":\"yma_zacy.ah=yma_zb.ah\",\"xgsj\":\"0000000000000000\"},"
//			+ "{\"table\":\"wjp_bc\",\"join\":[\"wjp_zb\"],\"key\":\"wjp_bc.wjpid=wjp_zb.wjpid\",\"xgsj\":\"0000000000000000\"},"
//			+ "{\"table\":\"wyld_ry\",\"join\":[\"wyld_zb\"],\"key\":\"wyld_ry.wydid=wyld_zb.wydid\",\"xgsj\":\"0000000000000000\"},"
//			+ "{\"table\":\"hj\",\"join\":[\"hj_fb\"],\"key\":\"hj.hjid=hj_fb.hjid\",\"xgsj\":\"0000000000000000\"},{\"table\":\"khjf\",\"xgsj\":\"0000000000000000\"},"
//			+ "{\"table\":\"khjf_sd\",\"xgsj\":\"0000000000000000\"},{\"table\":\"khf\",\"xgsj\":\"0000000000000000\"},{\"table\":\"thdj\",\"xgsj\":\"0000000000000000\"},"
//			+ "{\"table\":\"wp_bgzb\",\"join\":[\"wp_bgbc\"],\"key\":\"wp_bgzb.bh=wp_bgbc.bh AND wp_bgzb.djrq=wp_bgbc.djrq\",\"xgsj\":\"0000000000000000\"},"
//			+ "{\"table\":\"wwzk\",\"xgsj\":\"0000000000000000\"},{\"table\":\"wwjc\",\"xgsj\":\"0000000000000000\"},"
//			+ "{\"table\":\"wwbx\",\"join\":[\"wwzk\"],\"key\":\"wwbx.bh=wwzk.bh AND wwbx.pzrq=wwzk.pzrq\",\"xgsj\":\"0000000000000000\"},{\"table\":\"sndd\",\"xgsj\":\"0000000000000000\"}]}]}";

	public static String documentDBForSqliteInitJson = "{\"databases\":[{\"db\":\"DocumentDBForSqlite\",\"rowversion\":\"xgsj2\",\"tables\":"
			+ "[{\"table\":\"da_jbxx\",\"xgsj\":\"0\"},{\"table\":\"da_jl\",\"xgsj\":\"0\"},{\"table\":\"da_qklj\",\"xgsj\":\"0\"},{\"table\":\"da_shgx\",\"xgsj\":\"0\"},"
			+ "{\"table\":\"da_tzzb\",\"xgsj\":\"0\"},{\"table\":\"da_tszb\",\"join\":[\"da_tsbc\"],\"key\":\"da_tszb.bh=da_tsbc.bh\",\"xgsj\":\"0\"},"
			+ "{\"table\":\"bwxb\",\"xgsj\":\"0\"},{\"table\":\"tt\",\"xgsj\":\"0\"},{\"table\":\"lbc\",\"xgsj\":\"0\"},{\"table\":\"ss\",\"join\":[\"ssfb\"],\"key\":"
			+ "\"ss.ssid=ssfb.ssid\",\"xgsj\":\"0\"},{\"table\":\"ks\",\"join\":[\"ksfb\"],\"key\":\"ks.bh=ksfb.bh AND ks.ksrq=ksfb.ksrq\",\"xgsj\":\"0\"},"
			+ "{\"table\":\"jfjjzb\",\"xgsj\":\"0\"},{\"table\":\"tbjd\",\"xgsj\":\"0\"},{\"table\":\"gzjs\",\"xgsj\":\"0\"},{\"table\":\"qjfj\",\"xgsj\":\"0\"},"
			+ "{\"table\":\"yjdj\",\"xgsj\":\"0\"},{\"table\":\"sg\",\"xgsj\":\"0\"},{\"table\":\"em_zb\",\"xgsj\":\"0\"},{\"table\":\"em_qk\",\"join\":[\"em_zb\"],\"key\":"
			+ "\"em_qk.bh=em_zb.bh\",\"xgsj\":\"0\"},{\"table\":\"em_jd\",\"join\":[\"em_zb\"],\"key\":\"em_jd.bh=em_zb.bh\",\"xgsj\":\"0\"},{\"table\":\"em_jc\",\"join\":"
			+ "[\"em_zb\"],\"key\":\"em_jc.bh=em_zb.bh\",\"xgsj\":\"0\"},{\"table\":\"em_sy\",\"join\":[\"em_zb\"],\"key\":\"em_sy.bh=em_zb.bh\",\"xgsj\":\"0\"},"
			+ "{\"table\":\"fpa_zacy\",\"join\":[\"fpa_zb\",\"fpa_swry\"],\"key\":\"fpa_zacy.ah=fpa_zb.ah AND fpa_zacy.ah=fpa_swry.ah\",\"xgsj\":\"0\"},"
			+ "{\"table\":\"yma_zacy\",\"join\":[\"yma_zb\"],\"key\":\"yma_zacy.ah=yma_zb.ah\",\"xgsj\":\"0\"},{\"table\":\"wjp_bc\",\"join\":[\"wjp_zb\"],\"key\":"
			+ "\"wjp_bc.wjpid=wjp_zb.wjpid\",\"xgsj\":\"0\"},{\"table\":\"wyld_ry\",\"join\":[\"wyld_zb\"],\"key\":\"wyld_ry.wydid=wyld_zb.wydid\",\"xgsj\":\"0\"},"
			+ "{\"table\":\"hj\",\"join\":[\"hj_fb\"],\"key\":\"hj.hjid=hj_fb.hjid\",\"xgsj\":\"0\"},{\"table\":\"khjf\",\"xgsj\":\"0\"},{\"table\":\"khjf_sd\",\"xgsj\":\"0\"},"
			+ "{\"table\":\"khf\",\"xgsj\":\"0\"},{\"table\":\"thdj\",\"xgsj\":\"0\"},{\"table\":\"wp_bgzb\",\"join\":[\"wp_bgbc\"],\"key\":"
			+ "\"wp_bgzb.bh=wp_bgbc.bh AND wp_bgzb.djrq=wp_bgbc.djrq\",\"xgsj\":\"0\"},{\"table\":\"wwzk\",\"xgsj\":\"0\"},{\"table\":\"wwjc\",\"xgsj\":\"0\"},"
			+ "{\"table\":\"wwbx\",\"join\":[\"wwzk\"],\"key\":\"wwbx.bh=wwzk.bh AND wwbx.pzrq=wwzk.pzrq\",\"xgsj\":\"0\"},{\"table\":\"sndd\",\"xgsj\":\"0\"}]}]}";

//	public static String documentDBInitJson = "{\"databases\":[{\"db\":\"DocumentDB\",\"rowversion\":\"xgsj\",\"tables\":"
//		+ "[{\"table\":\"da_jbxx\",\"xgsj\":\"0\"},{\"table\":\"daFILE_UPLOAD_RETRY_TIMES_jl\",\"xgsj\":\"0\"},{\"table\":\"da_qklj\",\"xgsj\":\"0\"},{\"table\":\"da_shgx\",\"xgsj\":\"0\"},"
//		+ "{\"table\":\"da_tzzb\",\"xgsj\":\"0\"},{\"table\":\"da_tszb\",\"join\":[\"da_tsbc\"],\"key\":\"da_tszb.bh=da_tsbc.bh\",\"xgsj\":\"0\"},"
//		+ "{\"table\":\"bwxb\",\"xgsj\":\"0\"},{\"table\":\"tt\",\"xgsj\":\"0\"},{\"table\":\"lbc\",\"xgsj\":\"0\"},{\"table\":\"ss\",\"join\":[\"ssfb\"],\"key\":"
//		+ "\"ss.ssid=ssfb.ssid\",\"xgsj\":\"0\"},{\"table\":\"ks\",\"join\":[\"ksfb\"],\"key\":\"ks.bh=ksfb.bh AND ks.ksrq=ksfb.ksrq\",\"xgsj\":\"0\"},"
//		+ "{\"table\":\"jfjjzb\",\"xgsj\":\"0\"},{\"table\":\"tbjd\",\"xgsj\":\"0\"},{\"table\":\"gzjs\",\"xgsj\":\"0\"},{\"table\":\"qjfj\",\"xgsj\":\"0\"},"
//		+ "{\"table\":\"yjdj\",\"xgsj\":\"0\"},{\"table\":\"sg\",\"xgsj\":\"0\"},{\"table\":\"em_zb\",\"xgsj\":\"0\"},{\"table\":\"em_qk\",\"join\":[\"em_zb\"],\"key\":"
//		+ "\"em_qk.bh=em_zb.bh\",\"xgsj\":\"0\"},{\"table\":\"em_jd\",\"join\":[\"em_zb\"],\"key\":\"em_jd.bh=em_zb.bh\",\"xgsj\":\"0\"},{\"table\":\"em_jc\",\"join\":"
//		+ "[\"em_zb\"],\"key\":\"em_jc.bh=em_zb.bh\",\"xgsj\":\"0\"},{\"table\":\"em_sy\",\"join\":[\"em_zb\"],\"key\":\"em_sy.bh=em_zb.bh\",\"xgsj\":\"0\"},"
//		+ "{\"table\":\"fpa_zacy\",\"join\":[\"fpa_zb\",\"fpa_swry\"],\"key\":\"fpa_zacy.ah=fpa_zb.ah AND fpa_zacy.ah=fpa_swry.ah\",\"xgsj\":\"0\"},"
//		+ "{\"table\":\"yma_zacy\",\"join\":[\"yma_zb\"],\"key\":\"yma_zacy.ah=yma_zb.ah\",\"xgsj\":\"0\"},{\"table\":\"wjp_bc\",\"join\":[\"wjp_zb\"],\"key\":"
//		+ "\"wjp_bc.wjpid=wjp_zb.wjpid\",\"xgsj\":\"0\"},{\"table\":\"wyld_ry\",\"join\":[\"wyld_zb\"],\"key\":\"wyld_ry.wydid=wyld_zb.wydid\",\"xgsj\":\"0\"},"
//		+ "{\"table\":\"hj\",\"join\":[\"hj_fb\"],\"key\":\"hj.hjid=hj_fb.hjid\",\"xgsj\":\"0\"},{\"table\":\"khjf\",\"xgsj\":\"0\"},{\"table\":\"khjf_sd\",\"xgsj\":\"0\"},"
//		+ "{\"table\":\"khf\",\"xgsj\":\"0\"},{\"table\":\"thdj\",\"xgsj\":\"0\"},{\"table\":\"wp_bgzb\",\"join\":[\"wp_bgbc\"],\"key\":"
//		+ "\"wp_bgzb.bh=wp_bgbc.bh AND wp_bgzb.djrq=wp_bgbc.djrq\",\"xgsj\":\"0\"},{\"table\":\"wwzk\",\"xgsj\":\"0\"},{\"table\":\"wwjc\",\"xgsj\":\"0\"},"
//		+ "{\"table\":\"wwbx\",\"join\":[\"wwzk\"],\"key\":\"wwbx.bh=wwzk.bh AND wwbx.pzrq=wwzk.pzrq\",\"xgsj\":\"0\"},{\"table\":\"sndd\",\"xgsj\":\"0\"}]},"
//		+ "{\"db\":\"MeetingDB\",\"rowversion\":\"starttime\",\"tables\":[{\"table\":\"pias/getItlist\",\"starttime\":\"" + fiveDayBefore + "\"}]},"
//		+ "{\"db\":\"TalkDB\",\"rowversion\":\"starttime\",\"tables\":[{\"table\":\"qqdh/getTalklist\",\"starttime\":\"" + fiveDayBefore + "\"},{\"table\":\"qqdh/getQqdh\",\"starttime\":\"" + fiveDayBefore + "\"}]},"
//		+ "{\"db\":\"PrasDB\",\"rowversion\":\"starttime\",\"tables\":[{\"table\":\"pras/getResult\",\"starttime\":\"" + fiveDayBefore + "\"},{\"table\":\"pras/getTable\",\"starttime\":\"" + fiveDayBefore + "\"}]},"
//		+ "{\"db\":\"JfkhDB\",\"rowversion\":\"ID\",\"tables\":[{\"table\":\"BZ_JFKH_DRECORDSUB\",\"join_subtable\":[\"BZ_JFKH_DRECORD\"],\"key\":"
//		+ "\"BZ_JFKH_DRECORDSUB.PID=BZ_JFKH_DRECORD.ID\",\"ID\":\"0\"},{\"table\":\"BZ_JFKH_MYZKJFSPSUB\",\"join_subtable\":[\"BZ_JFKH_MYZKJFSP\"],\"key\":"
//		+ "\"BZ_JFKH_MYZKJFSPSUB.PID=BZ_JFKH_MYZKJFSP.ID\",\"ID\":\"0\"},{\"table\":\"BZ_JFKH_ZFFJQDDJL\",\"ID\":\"0\"},{\"table\":\"BZ_JFKH_ZFFYDDJL\",\"ID\":\"0\"},"
//		+ "{\"table\":\"BZ_KHBZ_DOCTOR\",\"join_subtable\":[\"BZ_KHBZ_DOCTORSUB\"],\"key\":\"BZ_KHBZ_DOCTOR.ID=BZ_KHBZ_DOCTORSUB.PID\",\"ID\":\"0\"},"
//		+ "{\"table\":\"BZ_KHBZ_JBSP\",\"ID\":\"0\"},{\"table\":\"BZ_KHBZ_JJJSP\",\"ID\":\"0\"},{\"table\":\"BZ_KHBZ_LJTQSP\",\"join_subtable\":[\"BZ_KHBZ_LJTQSPSUB\"],"
//		+ "\"key\":\"BZ_KHBZ_LJTQSP.ID=BZ_KHBZ_LJTQSPSUB.PID\",\"ID\":\"0\"},{\"table\":\"BZ_KHBZ_TXLJTQSP\",\"ID\":\"0\"},{\"table\":\"BZ_KHBZ_XZCFSP\",\"ID\":\"0\"},"
//		+ "{\"table\":\"BZ_KHBZ_XZJLSP\",\"ID\":\"0\"}]}]}";

	public static String documentDBInitJson = "{\"databases\":[{\"db\":\"DocumentDB\",\"rowversion\":\"xgsj\",\"tables\":"
			+ "[{\"table\":\"da_jbxx\",\"xgsj\":\"0\"},{\"table\":\"da_jl\",\"xgsj\":\"0\"},{\"table\":\"da_qklj\",\"xgsj\":\"0\"},{\"table\":\"da_shgx\",\"xgsj\":\"0\"},"
			+ "{\"table\":\"da_tzzb\",\"xgsj\":\"0\"},{\"table\":\"da_tszb\",\"join\":[\"da_tsbc\"],\"key\":\"da_tszb.bh=da_tsbc.bh\",\"xgsj\":\"0\"},"
			+ "{\"table\":\"bwxb\",\"xgsj\":\"0\"},{\"table\":\"tt\",\"xgsj\":\"0\"},{\"table\":\"lbc\",\"xgsj\":\"0\"},{\"table\":\"ss\",\"join\":[\"ssfb\"],\"key\":"
			+ "\"ss.ssid=ssfb.ssid\",\"xgsj\":\"0\"},{\"table\":\"ks\",\"join\":[\"ksfb\"],\"key\":\"ks.bh=ksfb.bh AND ks.ksrq=ksfb.ksrq\",\"xgsj\":\"0\"},"
			+ "{\"table\":\"jfjjzb\",\"xgsj\":\"0\"},{\"table\":\"tbjd\",\"xgsj\":\"0\"},{\"table\":\"gzjs\",\"xgsj\":\"0\"},{\"table\":\"qjfj\",\"xgsj\":\"0\"},"
			+ "{\"table\":\"yjdj\",\"xgsj\":\"0\"},{\"table\":\"sg\",\"xgsj\":\"0\"},{\"table\":\"em_zb\",\"xgsj\":\"0\"},{\"table\":\"em_qk\",\"join\":[\"em_zb\"],\"key\":"
			+ "\"em_qk.bh=em_zb.bh\",\"xgsj\":\"0\"},{\"table\":\"em_jd\",\"join\":[\"em_zb\"],\"key\":\"em_jd.bh=em_zb.bh\",\"xgsj\":\"0\"},{\"table\":\"em_jc\",\"join\":"
			+ "[\"em_zb\"],\"key\":\"em_jc.bh=em_zb.bh\",\"xgsj\":\"0\"},{\"table\":\"em_sy\",\"join\":[\"em_zb\"],\"key\":\"em_sy.bh=em_zb.bh\",\"xgsj\":\"0\"},"
			+ "{\"table\":\"fpa_zacy\",\"join\":[\"fpa_zb\",\"fpa_swry\"],\"key\":\"fpa_zacy.ah=fpa_zb.ah AND fpa_zacy.ah=fpa_swry.ah\",\"xgsj\":\"0\"},"
			+ "{\"table\":\"yma_zacy\",\"join\":[\"yma_zb\"],\"key\":\"yma_zacy.ah=yma_zb.ah\",\"xgsj\":\"0\"},{\"table\":\"wjp_bc\",\"join\":[\"wjp_zb\"],\"key\":"
			+ "\"wjp_bc.wjpid=wjp_zb.wjpid\",\"xgsj\":\"0\"},{\"table\":\"wyld_ry\",\"join\":[\"wyld_zb\"],\"key\":\"wyld_ry.wydid=wyld_zb.wydid\",\"xgsj\":\"0\"},"
			+ "{\"table\":\"hj\",\"join\":[\"hj_fb\"],\"key\":\"hj.hjid=hj_fb.hjid\",\"xgsj\":\"0\"},{\"table\":\"khjf\",\"xgsj\":\"0\"},{\"table\":\"khjf_sd\",\"xgsj\":\"0\"},"
			+ "{\"table\":\"khf\",\"xgsj\":\"0\"},{\"table\":\"thdj\",\"xgsj\":\"0\"},{\"table\":\"wp_bgzb\",\"join\":[\"wp_bgbc\"],\"key\":"
			+ "\"wp_bgzb.bh=wp_bgbc.bh AND wp_bgzb.djrq=wp_bgbc.djrq\",\"xgsj\":\"0\"},{\"table\":\"wwzk\",\"xgsj\":\"0\"},{\"table\":\"wwjc\",\"xgsj\":\"0\"},"
			+ "{\"table\":\"wwbx\",\"join\":[\"wwzk\"],\"key\":\"wwbx.bh=wwzk.bh AND wwbx.pzrq=wwzk.pzrq\",\"xgsj\":\"0\"},{\"table\":\"sndd\",\"xgsj\":\"0\"}]},"
			+ "{\"db\":\"MeetingDB\",\"rowversion\":\"starttime\",\"tables\":[{\"table\":\"pias/getItlist\",\"starttime\":\"" + fiveDayBefore + "\"}]},"
			+ "{\"db\":\"TalkDB\",\"rowversion\":\"starttime\",\"tables\":[{\"table\":\"qqdh/getTalklist\",\"starttime\":\"" + fiveDayBefore + "\"},{\"table\":\"qqdh/getQqdh\",\"starttime\":\"" + fiveDayBefore + "\"}]},"
			+ "{\"db\":\"PrasDB\",\"rowversion\":\"starttime\",\"tables\":[{\"table\":\"pras/getResult\",\"starttime\":\"" + fiveDayBefore + "\"},{\"table\":\"pras/getTable\",\"starttime\":\"" + fiveDayBefore + "\"}]},"
			+ "{\"db\":\"JfkhDB\",\"rowversion\":\"ID\",\"tables\":[{\"table\":\"BZ_JFKH_DRECORDSUB\",\"join\":[\"BZ_JFKH_DRECORD\"],\"key\":\"BZ_JFKH_DRECORDSUB.PID=BZ_JFKH_DRECORD.ID\",\"ID\":\"0\"},"
			+ "{\"table\":\"BZ_JFKH_MYZKJFSPSUB\",\"join\":[\"BZ_JFKH_MYZKJFSP\"],\"key\":\"BZ_JFKH_MYZKJFSPSUB.PID=BZ_JFKH_MYZKJFSP.ID\",\"ID\":\"0\"},{\"table\":\"BZ_JFKH_ZFFJQDDJL\",\"ID\":\"0\"},"
			+ "{\"table\":\"BZ_JFKH_ZFFYDDJL\",\"ID\":\"0\"},{\"table\":\"BZ_KHBZ_DOCTOR\",\"join_subtable\":[\"BZ_KHBZ_DOCTORSUB\"],\"key\":\"BZ_KHBZ_DOCTOR.ID=BZ_KHBZ_DOCTORSUB.PID\",\"ID\":\"0\"},"
			+ "{\"table\":\"BZ_KHBZ_JBSP\",\"ID\":\"0\"},{\"table\":\"BZ_KHBZ_JJJSP\",\"ID\":\"0\"},{\"table\":\"BZ_KHBZ_LJTQSP\",\"join_subtable\":[\"BZ_KHBZ_LJTQSPSUB\"],\"key\":\"BZ_KHBZ_LJTQSP.ID=BZ_KHBZ_LJTQSPSUB.PID\",\"ID\":\"0\"},"
			+ "{\"table\":\"BZ_KHBZ_TXLJTQSP\",\"ID\":\"0\"},{\"table\":\"BZ_KHBZ_XZCFSP\",\"ID\":\"0\"},{\"table\":\"BZ_KHBZ_XZJLSP\",\"ID\":\"0\"}]},"
			+ "{\"db\":\"DocumentDBForSqlite\",\"rowversion\":\"xgsj2\",\"tables\":"
			+ "[{\"table\":\"da_jbxx\",\"xgsj\":\"0\"},{\"table\":\"da_jl\",\"xgsj\":\"0\"},{\"table\":\"da_qklj\",\"xgsj\":\"0\"},{\"table\":\"da_shgx\",\"xgsj\":\"0\"},"
			+ "{\"table\":\"da_tzzb\",\"xgsj\":\"0\"},{\"table\":\"da_tszb\",\"join\":[\"da_tsbc\"],\"key\":\"da_tszb.bh=da_tsbc.bh\",\"xgsj\":\"0\"},"
			+ "{\"table\":\"bwxb\",\"xgsj\":\"0\"},{\"table\":\"tt\",\"xgsj\":\"0\"},{\"table\":\"lbc\",\"xgsj\":\"0\"},{\"table\":\"ss\",\"join\":[\"ssfb\"],\"key\":"
			+ "\"ss.ssid=ssfb.ssid\",\"xgsj\":\"0\"},{\"table\":\"ks\",\"join\":[\"ksfb\"],\"key\":\"ks.bh=ksfb.bh AND ks.ksrq=ksfb.ksrq\",\"xgsj\":\"0\"},"
			+ "{\"table\":\"jfjjzb\",\"xgsj\":\"0\"},{\"table\":\"tbjd\",\"xgsj\":\"0\"},{\"table\":\"gzjs\",\"xgsj\":\"0\"},{\"table\":\"qjfj\",\"xgsj\":\"0\"},"
			+ "{\"table\":\"yjdj\",\"xgsj\":\"0\"},{\"table\":\"sg\",\"xgsj\":\"0\"},{\"table\":\"em_zb\",\"xgsj\":\"0\"},{\"table\":\"em_qk\",\"join\":[\"em_zb\"],\"key\":"
			+ "\"em_qk.bh=em_zb.bh\",\"xgsj\":\"0\"},{\"table\":\"em_jd\",\"join\":[\"em_zb\"],\"key\":\"em_jd.bh=em_zb.bh\",\"xgsj\":\"0\"},{\"table\":\"em_jc\",\"join\":"
			+ "[\"em_zb\"],\"key\":\"em_jc.bh=em_zb.bh\",\"xgsj\":\"0\"},{\"table\":\"em_sy\",\"join\":[\"em_zb\"],\"key\":\"em_sy.bh=em_zb.bh\",\"xgsj\":\"0\"},"
			+ "{\"table\":\"fpa_zacy\",\"join\":[\"fpa_zb\",\"fpa_swry\"],\"key\":\"fpa_zacy.ah=fpa_zb.ah AND fpa_zacy.ah=fpa_swry.ah\",\"xgsj\":\"0\"},"
			+ "{\"table\":\"yma_zacy\",\"join\":[\"yma_zb\"],\"key\":\"yma_zacy.ah=yma_zb.ah\",\"xgsj\":\"0\"},{\"table\":\"wjp_bc\",\"join\":[\"wjp_zb\"],\"key\":"
			+ "\"wjp_bc.wjpid=wjp_zb.wjpid\",\"xgsj\":\"0\"},{\"table\":\"wyld_ry\",\"join\":[\"wyld_zb\"],\"key\":\"wyld_ry.wydid=wyld_zb.wydid\",\"xgsj\":\"0\"},"
			+ "{\"table\":\"hj\",\"join\":[\"hj_fb\"],\"key\":\"hj.hjid=hj_fb.hjid\",\"xgsj\":\"0\"},{\"table\":\"khjf\",\"xgsj\":\"0\"},{\"table\":\"khjf_sd\",\"xgsj\":\"0\"},"
			+ "{\"table\":\"khf\",\"xgsj\":\"0\"},{\"table\":\"thdj\",\"xgsj\":\"0\"},{\"table\":\"wp_bgzb\",\"join\":[\"wp_bgbc\"],\"key\":"
			+ "\"wp_bgzb.bh=wp_bgbc.bh AND wp_bgzb.djrq=wp_bgbc.djrq\",\"xgsj\":\"0\"},{\"table\":\"wwzk\",\"xgsj\":\"0\"},{\"table\":\"wwjc\",\"xgsj\":\"0\"},"
			+ "{\"table\":\"wwbx\",\"join\":[\"wwzk\"],\"key\":\"wwbx.bh=wwzk.bh AND wwbx.pzrq=wwzk.pzrq\",\"xgsj\":\"0\"},{\"table\":\"sndd\",\"xgsj\":\"0\"}]},"
			+ "{\"db\":\"VideoMeetingDB\",\"rowversion\":\"ID\",\"tables\":"
			+ "[{\"table\":\"MeetingApplies\",\"ID\":\"" + 0 + "\", \"join\":[\"UserAccounts\", \"Prisoner\", \"Users\", \"Departments\", \"Jails\"],"
			+ "\"key\":\" MeetingApplies.PrisonerFk = Prisoner.UserFk and MeetingApplies.CreateUserFk = UserAccounts.Id and Prisoner.UserFk = Users.Id and Users.DepartmentFk = Departments.Id and Users.JailFk = Jails.Id\"}]},"
			+ "{\"db\":\"HelpDB\",\"rowversion\":\"ID\",\"tables\":"
			+ "[{\"table\":\"Fee_UserCharges\",\"ID\":\"" + 0 + "\", \"join\":[\"Fee_UserAccounts\", \"Users\", \"Departments\"],"
			+ "\"key\":\"  Fee_UserCharges.UserFk = Users.Id and Fee_UserCharges.UserAccountFk = Fee_UserAccounts.Id and Users.DepartmentFk = Departments.Id\"},"
			+ "{\"table\":\"Fee_UserDeductions\",\"ID\":\"" + 0 + "\", \"join\":[\"Users\", \"Departments\"],\"key\":\"Fee_UserDeductions.UserFk = Users.Id and Users.DepartmentFk = Departments.Id\"},"
			+ "{\"table\":\"Contacts\",\"ID\":\"" + 0 + "\", \"join\":[\"Prisoner\", \"Users\", \"Departments\", \"CommonCodes\"],"
			+ "\"key\":\"Contacts.PrisonerFk = Prisoner.UserFk and Prisoner.UserFk = Users.Id and Contacts.RelationFk = CommonCodes.Id and Users.DepartmentFk = Departments.Id\"},"
			+ "{\"table\":\"Sms_SmsSendBoxes\",\"ID\":\"" + 0 + "\", \"join\":[\"Prisoner\", \"Users\", \"Departments\", \"CommonCodes\"],"
			+ "\"key\":\"Sms_SmsSendBoxes.PrisonerFk = Prisoner.UserFk and Prisoner.UserFk = Users.Id and Sms_SmsSendBoxes.FailureReasonFk = CommonCodes.Id and Sms_SmsSendBoxes.FeelingFk = CommonCodes.Id "
			+ "and Sms_SmsSendBoxes.RelationFk = CommonCodes.Id and Users.DepartmentFk = Departments.Id\"},"
			+ "{\"table\":\"Sms_SmsReceiveBoxes\",\"ID\":\"" + 0 + "\", \"join\":[\"Prisoner\", \"Users\", \"Departments\", \"CommonCodes\"],"
			+ "\"key\":\"Sms_SmsReceiveBoxes.PrisonerFk = Prisoner.UserFk and Prisoner.UserFk = Users.Id and Sms_SmsReceiveBoxes.FailureReasonFk = CommonCodes.Id "
			+ "and Sms_SmsReceiveBoxes.RelationFk = CommonCodes.Id and Users.DepartmentFk = Departments.Id\"},"
			+ "{\"table\":\"ClassRoom_Histories\",\"ID\":\"" + 0 + "\", \"join\":[\"ClassRoom_Videos\", \"Users\", \"Departments\", \"CommonCodes\"],"
			+ "\"key\":\"ClassRoom_Histories.UserFk = Users.Id and ClassRoom_Histories.VideoFk = ClassRoom_Videos.Id and ClassRoom_Videos.TypeFk = CommonCodes.Id and Users.DepartmentFk = Departments.Id\"},"
			+ "{\"table\":\"ClassRoom_StudyNotes\",\"ID\":\"" + 0 + "\", \"join\":[\"ClassRoom_Videos\", \"Users\", \"Departments\", \"CommonCodes\"],"
			+ "\"key\":\"ClassRoom_StudyNotes.UserFk = Users.Id and ClassRoom_StudyNotes.VideoFk = ClassRoom_Videos.Id and ClassRoom_Videos.TypeFk = CommonCodes.Id and Users.DepartmentFk = Departments.Id\"},"
			+ "{\"table\":\"Library_BookHistories\",\"ID\":\"" + 0 + "\", \"join\":[\"Library_Books\", \"Users\", \"Departments\", \"CommonCodes\"],"
			+ "\"key\":\"Library_BookHistories.UserFk = Users.Id and Library_BookHistories.BookFk = Library_Books.Id and Library_Books.CatalogFk = CommonCodes.Id and Users.DepartmentFk = Departments.Id\"},"
			+ "{\"table\":\"Library_BookReviews\",\"ID\":\"" + 0 + "\", \"join\":[\"Library_Books\", \"Users\", \"Departments\", \"CommonCodes\"],"
			+ "\"key\":\"Library_BookReviews.UserFk = Users.Id and Library_BookReviews.BookFk = Library_Books.Id and Library_Books.CatalogFk = CommonCodes.Id and Users.DepartmentFk = Departments.Id\"},"
			+ "{\"table\":\"Vod_VideoHistories\",\"ID\":\"" + 0 + "\", \"join\":[\"Vod_Videos\", \"Users\", \"Departments\", \"CommonCodes\"],"
			+ "\"key\":\"Vod_VideoHistories.UserFk = Users.Id and Vod_VideoHistories.VideoFk = Vod_Videos.Id and Vod_Videos.CatalogFk = CommonCodes.Id and Users.DepartmentFk = Departments.Id\"},"
			+ "{\"table\":\"Vod_VideoReviews\",\"ID\":\"" + 0 + "\", \"join\":[\"Vod_Videos\", \"Users\", \"Departments\", \"CommonCodes\"],"
			+ "\"key\":\"Vod_VideoReviews.UserFk = Users.Id and Vod_VideoReviews.VideoFk = Vod_Videos.Id and Vod_Videos.CatalogFk = CommonCodes.Id and Users.DepartmentFk = Departments.Id\"},"
			+ "{\"table\":\"Terminal_TerminalApplications\",\"ID\":\"" + 0 + "\", \"join\":[\"Terminal_SubscribeInfos\", \"TerminalApplications_SubscribeInfos\", "
			+ "\"Terminal_TerminalInfos\", \"Terminal_TerminalModels\", \"Users\", \"Departments\"],"
			+ "\"key\":\"Terminal_TerminalApplications.UserFk = Users.Id and Terminal_TerminalApplications.TerminalFk = Terminal_TerminalInfos.Id "
			+ "and Terminal_TerminalApplications.TerminalModelFk = Terminal_TerminalModels.Id and TerminalApplications_SubscribeInfos.SubscribeInfoFk = Terminal_SubscribeInfos.Id "
			+ "and TerminalApplications_SubscribeInfos.TerminalApplicationFk = Terminal_TerminalApplications.Id and Users.DepartmentFk = Departments.Id\"},"
			+ "{\"table\":\"Messages\",\"ID\":\"" + 0 + "\", \"join\":[\"Users\", \"Departments\"],\"key\":\"Messages.UserFk = Users.Id and Users.DepartmentFk = Departments.Id\"},"
			+ "{\"table\":\"RainGlass_UserEmotions\",\"ID\":\"" + 0 + "\", \"join\":[\"RainGlass_Emotions\", \"RainGlass_EmotionPersuasions\", \"Users\", \"Departments\"],"
			+ "\"key\":\"RainGlass_UserEmotions.UserFk = Users.Id and RainGlass_UserEmotions.EmotionFk = RainGlass_Emotions.Id "
			+ "and RainGlass_Emotions.Id = RainGlass_EmotionPersuasions.EmotionFk and Users.DepartmentFk = Departments.Id\"}]}]}";

//	private static String documentDBInitJson = "{\"databases\":[{\"db\":\"DocumentDB\",\"rowversion\":\"xgsj\",\"tables\":["
//		+ "{\"table\":\"da_jbxx\",\"xgsj\":\"0\"},{\"table\":\"da_jl\",\"xgsj\":\"0\"},{\"table\":\"da_qklj\",\"xgsj\":\"0\"},"
//		+ "{\"table\":\"da_shgx\",\"xgsj\":\"0\"},{\"table\":\"da_tzzb\",\"xgsj\":\"0\"},{\"table\":\"da_tszb\",\"join\":[\"da_tsbc\"],\"key\":\"da_tszb.bh=da_tsbc.bh\",\"xgsj\":\"0\"},"
//        + "{\"table\":\"da_clzl\",\"xgsj\":\"0\"},{\"table\":\"da_crj\",\"xgsj\":\"0\"},{\"table\":\"da_swdj\",\"xgsj\":\"0\"},{\"table\":\"da_tc\",\"xgsj\":\"0\"},"
//        + "{\"table\":\"da_zm\",\"xgsj\":\"0\"},{\"table\":\"yzjc\",\"xgsj\":\"0\"},{\"table\":\"xfzb\",\"xgsj\":\"0\"},{\"table\":\"djbd\",\"xgsj\":\"0\"},"
//        + "{\"table\":\"db\",\"xgsj\":\"0\"},{\"table\":\"nwfg\",\"xgsj\":\"0\"},{\"table\":\"hjdd\",\"xgsj\":\"0\"},{\"table\":\"hjbd\",\"xgsj\":\"0\"},"
//        + "{\"table\":\"jxcd\",\"xgsj\":\"0\"},{\"table\":\"st\",\"xgsj\":\"0\"},{\"table\":\"jy_rzfpbd\",\"xgsj\":\"0\"},{\"table\":\"jwbw\",\"xgsj\":\"0\"},"
//		+ "{\"table\":\"bwxb\",\"xgsj\":\"0\"},{\"table\":\"tt\",\"xgsj\":\"0\"},{\"table\":\"lbc\",\"xgsj\":\"0\"},{\"table\":\"ss\",\"join\":[\"ssfb\"],\"key\":\"ss.ssid=ssfb.ssid\",\"xgsj\":\"0\"},"
//		+ "{\"table\":\"ks\",\"join\":[\"ksfb\"],\"key\":\"ks.bh=ksfb.bh AND ks.ksrq=ksfb.ksrq\",\"xgsj\":\"0\"},{\"table\":\"jfjjzb\",\"xgsj\":\"0\"},{\"table\":\"tbjd\",\"xgsj\":\"0\"},"
//		+ "{\"table\":\"gzjs\",\"xgsj\":\"0\"},{\"table\":\"qjfj\",\"xgsj\":\"0\"},{\"table\":\"yjdj\",\"xgsj\":\"0\"},{\"table\":\"sg\",\"xgsj\":\"0\"},{\"table\":\"em_zb\",\"xgsj\":\"0\"},"
//		+ "{\"table\":\"em_qk\",\"join\":[\"em_zb\"],\"key\":\"em_qk.bh=em_zb.bh\",\"xgsj\":\"0\"},{\"table\":\"em_jd\",\"join\":[\"em_zb\"],\"key\":\"em_jd.bh=em_zb.bh\",\"xgsj\":\"0\"},"
//		+ "{\"table\":\"em_jc\",\"join\":[\"em_zb\"],\"key\":\"em_jc.bh=em_zb.bh\",\"xgsj\":\"0\"},{\"table\":\"em_sy\",\"join\":[\"em_zb\"],\"key\":\"em_sy.bh=em_zb.bh\",\"xgsj\":\"0\"},"
//		+ "{\"table\":\"fpa_zacy\",\"join\":[\"fpa_zb\",\"fpa_swry\"],\"key\":\"fpa_zacy.ah=fpa_zb.ah AND fpa_zacy.ah=fpa_swry.ah\",\"xgsj\":\"0\"},"
//		+ "{\"table\":\"yma_zacy\",\"join\":[\"yma_zb\"],\"key\":\"yma_zacy.ah=yma_zb.ah\",\"xgsj\":\"0\"},{\"table\":\"wjp_bc\",\"join\":[\"wjp_zb\"],\"key\":\"wjp_bc.wjpid=wjp_zb.wjpid\",\"xgsj\":\"0\"},"
//		+ "{\"table\":\"wyld_ry\",\"join\":[\"wyld_zb\"],\"key\":\"wyld_ry.wydid=wyld_zb.wydid\",\"xgsj\":\"0\"},{\"table\":\"hj\",\"join\":[\"hj_fb\"],\"key\":\"hj.hjid=hj_fb.hjid\",\"xgsj\":\"0\"},"
//		+ "{\"table\":\"khjf\",\"xgsj\":\"0\"},{\"table\":\"khjf_sd\",\"xgsj\":\"0\"},{\"table\":\"khf\",\"xgsj\":\"0\"},{\"table\":\"thdj\",\"xgsj\":\"0\"},"
//		+ "{\"table\":\"wp_bgzb\",\"join\":[\"wp_bgbc\"],\"key\":\"wp_bgzb.bh=wp_bgbc.bh AND wp_bgzb.djrq=wp_bgbc.djrq\",\"xgsj\":\"0\"},{\"table\":\"wwzk\",\"xgsj\":\"0\"},"
//		+ "{\"table\":\"wwjc\",\"xgsj\":\"0\"},{\"table\":\"wwbx\",\"join\":[\"wwzk\"],\"key\":\"wwbx.bh=wwzk.bh AND wwbx.pzrq=wwzk.pzrq\",\"xgsj\":\"0\"},{\"table\":\"sndd\",\"xgsj\":\"0\"}]}]}";

//	private static String youDiInitJson = "{\"databases\":[{\"db\":\"MeetingDB\",\"rowversion\":\"starttime\",\"tables\":[{\"table\":\"pias/getItlist\",\"starttime\":\"0\"}]},"
//			+ "{\"db\":\"TalkDB\",\"rowversion\":\"starttime\",\"tables\":[{\"table\":\"qqdh/getTalklist\",\"starttime\":\"0\"},{\"table\":\"qqdh/getQqdh\",\"starttime\":\"0\"}]},"
//			+ "{\"db\":\"PrasDB\",\"rowversion\":\"starttime\",\"tables\":[{\"table\":\"pras/getResult\",\"starttime\":\"0\"},{\"table\":\"pras/getTable\",\"starttime\":\"0\"}]}]}";

	/*
	 * web service db
	 */
	private static String youDiInitJson = "{\"databases\":[{\"db\":\"MeetingDB\",\"rowversion\":\"starttime\",\"tables\":[{\"table\":\"pias/getItlist\",\"starttime\":\"" + fiveDayBefore + "\"}]},"
			+ "{\"db\":\"TalkDB\",\"rowversion\":\"starttime\",\"tables\":[{\"table\":\"qqdh/getTalklist\",\"starttime\":\"" + fiveDayBefore + "\"},{\"table\":\"qqdh/getQqdh\",\"starttime\":\"" + fiveDayBefore + "\"}]},"
			+ "{\"db\":\"PrasDB\",\"rowversion\":\"starttime\",\"tables\":[{\"table\":\"pras/getResult\",\"starttime\":\"" + fiveDayBefore + "\"},{\"table\":\"pras/getTable\",\"starttime\":\"" + fiveDayBefore + "\"}]}]}";

	/*
	 * oracle db
	 */
	private static String zhongCiInitJson = "{\"databases\":[{\"db\":\"JfkhDB\",\"rowversion\":\"ID\",\"tables\":["
			+ "{\"table\":\"BZ_KHBZ_JBSP\",\"ID\":\"0\"},{\"table\":\"BZ_KHBZ_JJJSP\",\"ID\":\"0\"},"
			+ "{\"table\":\"BZ_JFKH_DRECORDSUB\",\"join\":[\"BZ_JFKH_DRECORD\"],\"key\":\"BZ_JFKH_DRECORDSUB.PID=BZ_JFKH_DRECORD.ID\",\"ID\":\"0\"},"
			+ "{\"table\":\"BZ_KHBZ_DOCTOR\",\"join_subtable\":[\"BZ_KHBZ_DOCTORSUB\"],\"key\":\"BZ_KHBZ_DOCTOR.ID=BZ_KHBZ_DOCTORSUB.PID\",\"ID\":\"0\"},"
			+ "{\"table\":\"BZ_JFKH_MYZKJFSPSUB\",\"join\":[\"BZ_JFKH_MYZKJFSP\"],\"key\":\"BZ_JFKH_MYZKJFSPSUB.PID=BZ_JFKH_MYZKJFSP.ID\",\"ID\":\"0\"},"
			+ "{\"table\":\"BZ_JFKH_ZFFJQDDJL\",\"ID\":\"0\"},{\"table\":\"BZ_JFKH_ZFFYDDJL\",\"ID\":\"0\"},"
			+ "{\"table\":\"BZ_KHBZ_LJTQSP\",\"join_subtable\":[\"BZ_KHBZ_LJTQSPSUB\"],\"key\":\"BZ_KHBZ_LJTQSP.ID=BZ_KHBZ_LJTQSPSUB.PID\",\"ID\":\"0\"},"
			+ "{\"table\":\"BZ_KHBZ_TXLJTQSP\",\"ID\":\"0\"},{\"table\":\"BZ_KHBZ_XZCFSP\",\"ID\":\"0\"},{\"table\":\"BZ_KHBZ_XZJLSP\",\"ID\":\"0\"}]}]}";

	/*
	 * sql server 2008
	 */
	private static String bangjiaoInitJson = "{\"databases\":["
			+ "{\"db\":\"VideoMeetingDB\",\"rowversion\":\"ID\",\"tables\":["
				+ "{\"table\":\"MeetingApplies\",\"ID\":\"" + 0 + "\", \"join\":[\"UserAccounts\", \"Prisoner\", \"Users\", \"Departments\", \"Jails\"],"
					+ "\"key\":\" MeetingApplies.PrisonerFk = Prisoner.UserFk and MeetingApplies.CreateUserFk = UserAccounts.Id "
						+ "and Prisoner.UserFk = Users.Id and Users.DepartmentFk = Departments.Id and Users.JailFk = Jails.Id\"}"
				+"]}"

			+ ",{\"db\":\"HelpDB\",\"rowversion\":\"ID\",\"tables\":["
				+ "{\"table\":\"Fee_UserCharges\",\"ID\":\"" + 0 + "\", \"join\":[\"Fee_UserAccounts\", \"Users\", \"Departments\"],"
					+ "\"key\":\"  Fee_UserCharges.UserFk = Users.Id and Fee_UserCharges.UserAccountFk = Fee_UserAccounts.Id "
						+ "and Users.DepartmentFk = Departments.Id\"},"
				+ "{\"table\":\"Fee_UserDeductions\",\"ID\":\"" + 0 + "\", \"join\":[\"Users\", \"Departments\"],"
					+ "\"key\":\"Fee_UserDeductions.UserFk = Users.Id and Users.DepartmentFk = Departments.Id\"},"
				+ "{\"table\":\"Contacts\",\"ID\":\"" + 0 + "\", \"join\":[\"Prisoner\", \"Users\", \"Departments\", \"CommonCodes\"],"
					+ "\"key\":\"Contacts.PrisonerFk = Prisoner.UserFk and Prisoner.UserFk = Users.Id "
						+ "and Contacts.RelationFk = CommonCodes.Id and Users.DepartmentFk = Departments.Id\"},"
				+ "{\"table\":\"Sms_SmsSendBoxes\",\"ID\":\"" + 0 + "\", \"join\":[\"Prisoner\", \"Users\", \"Departments\", \"CommonCodes\"],"
					+ "\"key\":\"Sms_SmsSendBoxes.PrisonerFk = Prisoner.UserFk and Prisoner.UserFk = Users.Id "
						+ "and Sms_SmsSendBoxes.FailureReasonFk = CommonCodes.Id and Sms_SmsSendBoxes.FeelingFk = CommonCodes.Id "
							+ "and Sms_SmsSendBoxes.RelationFk = CommonCodes.Id and Users.DepartmentFk = Departments.Id\"},"
				+ "{\"table\":\"Sms_SmsReceiveBoxes\",\"ID\":\"" + 0 + "\", \"join\":[\"Prisoner\", \"Users\", \"Departments\", \"CommonCodes\"],"
					+ "\"key\":\"Sms_SmsReceiveBoxes.PrisonerFk = Prisoner.UserFk and Prisoner.UserFk = Users.Id "
						+ "and Sms_SmsReceiveBoxes.FailureReasonFk = CommonCodes.Id and Sms_SmsReceiveBoxes.RelationFk = CommonCodes.Id "
							+ "and Users.DepartmentFk = Departments.Id\"},"
				+ "{\"table\":\"ClassRoom_Histories\",\"ID\":\"" + 0 + "\", \"join\":[\"ClassRoom_Videos\", \"Users\", \"Departments\", \"CommonCodes\"],"
					+ "\"key\":\"ClassRoom_Histories.UserFk = Users.Id and ClassRoom_Histories.VideoFk = ClassRoom_Videos.Id "
						+ "and ClassRoom_Videos.TypeFk = CommonCodes.Id and Users.DepartmentFk = Departments.Id\"},"
				+ "{\"table\":\"ClassRoom_StudyNotes\",\"ID\":\"" + 0 + "\", \"join\":[\"ClassRoom_Videos\", \"Users\", \"Departments\", \"CommonCodes\"],"
					+ "\"key\":\"ClassRoom_StudyNotes.UserFk = Users.Id and ClassRoom_StudyNotes.VideoFk = ClassRoom_Videos.Id "
						+ "and ClassRoom_Videos.TypeFk = CommonCodes.Id and Users.DepartmentFk = Departments.Id\"},"
				+ "{\"table\":\"Library_BookHistories\",\"ID\":\"" + 0 + "\", \"join\":[\"Library_Books\", \"Users\", \"Departments\", \"CommonCodes\"],"
					+ "\"key\":\"Library_BookHistories.UserFk = Users.Id and Library_BookHistories.BookFk = Library_Books.Id "
						+ "and Library_Books.CatalogFk = CommonCodes.Id and Users.DepartmentFk = Departments.Id\"},"
				+ "{\"table\":\"Library_BookReviews\",\"ID\":\"" + 0 + "\", \"join\":[\"Library_Books\", \"Users\", \"Departments\", \"CommonCodes\"],"
					+ "\"key\":\"Library_BookReviews.UserFk = Users.Id and Library_BookReviews.BookFk = Library_Books.Id "
						+ "and Library_Books.CatalogFk = CommonCodes.Id and Users.DepartmentFk = Departments.Id\"},"
				+ "{\"table\":\"Vod_VideoHistories\",\"ID\":\"" + 0 + "\", \"join\":[\"Vod_Videos\", \"Users\", \"Departments\", \"CommonCodes\"],"
					+ "\"key\":\"Vod_VideoHistories.UserFk = Users.Id and Vod_VideoHistories.VideoFk = Vod_Videos.Id "
						+ "and Vod_Videos.CatalogFk = CommonCodes.Id and Users.DepartmentFk = Departments.Id\"},"
				+ "{\"table\":\"Vod_VideoReviews\",\"ID\":\"" + 0 + "\", \"join\":[\"Vod_Videos\", \"Users\", \"Departments\", \"CommonCodes\"],"
					+ "\"key\":\"Vod_VideoReviews.UserFk = Users.Id and Vod_VideoReviews.VideoFk = Vod_Videos.Id "
						+ "and Vod_Videos.CatalogFk = CommonCodes.Id and Users.DepartmentFk = Departments.Id\"},"
				+ "{\"table\":\"Terminal_TerminalApplications\",\"ID\":\"" + 0 + "\", \"join\":[\"Terminal_SubscribeInfos\", \"TerminalApplications_SubscribeInfos\", "
					+ "\"Terminal_TerminalInfos\", \"Terminal_TerminalModels\", \"Users\", \"Departments\"],"
						+ "\"key\":\"Terminal_TerminalApplications.UserFk = Users.Id and Terminal_TerminalApplications.TerminalFk = Terminal_TerminalInfos.Id "
							+ "and Terminal_TerminalApplications.TerminalModelFk = Terminal_TerminalModels.Id and TerminalApplications_SubscribeInfos.SubscribeInfoFk = Terminal_SubscribeInfos.Id "
								+ "and TerminalApplications_SubscribeInfos.TerminalApplicationFk = Terminal_TerminalApplications.Id and Users.DepartmentFk = Departments.Id\"},"
				+ "{\"table\":\"Messages\",\"ID\":\"" + 0 + "\", \"join\":[\"Users\", \"Departments\"],"
					+ "\"key\":\"Messages.UserFk = Users.Id and Users.DepartmentFk = Departments.Id\"},"
				+ "{\"table\":\"RainGlass_UserEmotions\",\"ID\":\"" + 0 + "\", \"join\":[\"RainGlass_Emotions\", \"RainGlass_EmotionPersuasions\", \"Users\", \"Departments\"],"
					+ "\"key\":\"RainGlass_UserEmotions.UserFk = Users.Id and RainGlass_UserEmotions.EmotionFk = RainGlass_Emotions.Id "
						+ "and RainGlass_Emotions.Id = RainGlass_EmotionPersuasions.EmotionFk and Users.DepartmentFk = Departments.Id\"}"
				+ "]}"
			+ "]}";

	/*
	 * file db
	 */
	private static String documentFilesInitJson = "{\"databases\":[{\"db\":\"DocumentFiles\",\"rowversion\":\"filetime\",\"tables\":"
			+ "[{\"table\":\"" + PROJECT_ABSOLUTE_PATH + "/src/resources/fileUploaderTestPics\",\"xgsj\":\"0000000000000000\"}]}]}";

	public static String getTableId() {
		return clientId;
	}

	public static MultiDatabaseBean getMultiDatabaseBean() throws JsonParseException, JsonMappingException, IOException{
		if(databaseBeans == null && clientId != null){
			ObjectMapper oMapper = new ObjectMapper();
			if (clientId.endsWith("db")) {
				//test all
				databaseBeans = oMapper.readValue(documentDBInitJson, MultiDatabaseBean.class);
				//for web server test
//				databaseBeans = oMapper.readValue(youDiInitJson, MultiDatabaseBean.class);
				//for sqlite
//				databaseBeans = oMapper.readValue(documentDBForSqliteInitJson, MultiDatabaseBean.class);
				//for oracle
//				databaseBeans = oMapper.readValue(zhongCiInitJson, MultiDatabaseBean.class);
				//for sql server2008
//				databaseBeans = oMapper.readValue(bangjiaoInitJson, MultiDatabaseBean.class);
			}
			else if(clientId.endsWith("documentfile")){
				databaseBeans = oMapper.readValue(documentFilesInitJson, MultiDatabaseBean.class);
			}
		}
		return databaseBeans;
	}

	public static void setMultiDatabaseBean(MultiDatabaseBean dbs) {
		databaseBeans = dbs;
	}

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException{
    	String url = req.getRequestURI();
    	int tableIdIndex = url.lastIndexOf('/');
    	if (url.length() <= getTaskApi.length() || tableIdIndex != (getTaskApi.length() - 1)) {
			throw new ServletException("invalid url, format: " + getTaskApi + "{tableId}");
		}

    	System.out.println("start get task succeed!");
    	
    	clientId = url.substring(tableIdIndex + 1);
    	String json;
    	if (clientId.endsWith("db")) {
    		databaseBeans = getMultiDatabaseBean();
    		for(int i = 0; i < databaseBeans.getDatabases().size(); i++){
    			DatabaseBean dBean = databaseBeans.getDatabases().get(i);
    			if(map.get(dBean.getDb()).equals("webservice")){
    				for(int j = 0; j < dBean.getTables().size(); j++){
        				TableBean tBean = dBean.getTables().get(j);
        				tBean.setID(tBean.getStarttime());
        			}
    			}
    			else if(map.get(dBean.getDb()).equals("oracle")){
    				for(int j = 0; j < dBean.getTables().size(); j++){
        				TableBean tBean = dBean.getTables().get(j);
        				tBean.setStarttime(tBean.getID());
        			}
    			}
    			else if(map.get(dBean.getDb()).equals("sqlserver")){
    				for(int j = 0; j < dBean.getTables().size(); j++){
        				TableBean tBean = dBean.getTables().get(j);
        				if (!tBean.getXgsj().startsWith("0x")) {
    						tBean.setXgsj("0x" + tBean.getXgsj());
    					}
        				tBean.setStarttime(tBean.getXgsj());
        				tBean.setID(tBean.getXgsj());
        			}
    			}
    			else if(map.get(dBean.getDb()).equals("sqlite")){
    				for(int j = 0; j < dBean.getTables().size(); j++){
        				TableBean tBean = dBean.getTables().get(j);
        				tBean.setStarttime(tBean.getXgsj());
        				tBean.setID(tBean.getXgsj());
        			}
    			}
    		}
    		ObjectMapper oMapper = new ObjectMapper();
    		StringWriter str=new StringWriter();
    		oMapper.writeValue(str, databaseBeans);
    		json = str.toString();
    		logger.info("task from server："+json);
//    		json = zhongCiInitJson;
//    		System.out.println("task from server："+json);
    	}else if (clientId.endsWith("documentfile")) {
//    		json = "{\"databases\":[{\"db\":\"DocumentFiles\",\"rowversion\":\"filetime\",\"tables\":[{\"table\":\"c://罪犯媒体/像片\",\"xgsj\":\"0000000000000000\"}]}]}";
    		databaseBeans = getMultiDatabaseBean();
    		for(int i = 0; i < databaseBeans.getDatabases().size(); i++){
    			DatabaseBean dBean = databaseBeans.getDatabases().get(i);
    			for(int j = 0; j < dBean.getTables().size(); j++){
    				TableBean tBean = dBean.getTables().get(j);
    				tBean.setStarttime(tBean.getXgsj());
    				tBean.setID(tBean.getXgsj());
    			}
    		}
    		ObjectMapper oMapper = new ObjectMapper();
    		StringWriter str=new StringWriter();
    		oMapper.writeValue(str, databaseBeans);
    		json = str.toString();
    		logger.info("task from server："+json);
    	}else {
    		json = "{\"databases\":[]}";
		}

    	//resp.setHeader(\"Content-type\", \"text/html;charset=UTF-8\");
    	resp.setCharacterEncoding("utf-8");
    	PrintWriter pw = resp.getWriter();
    	//pw.write(tableId);
        pw.write(json);
        pw.flush();
        pw.close();
        System.out.println("get task succeed!");
    }

    @Override
    protected void doDelete(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	super.doDelete(req, resp);
    }

    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	super.doPut(req, resp);
    }
    
    public static void main(String []args) {
		System.out.println(fourDayBeforeString);
		System.out.println(fourDayBefore);
		System.out.println(PROJECT_ABSOLUTE_PATH);
	}
}
