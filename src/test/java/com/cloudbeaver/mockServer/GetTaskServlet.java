package com.cloudbeaver.mockServer;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

@WebServlet("/api/business/sync/*")
public class GetTaskServlet extends HttpServlet{
	private static Logger logger = Logger.getLogger(GetTaskServlet.class);
	private static String getTaskApi = "/api/business/sync/";

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
    	
    	String tableId = url.substring(tableIdIndex + 1);
    	String json = "";
    	if (tableId.endsWith("documentdb")) {
        	json = "{\"databases\":[{\"db\":\"DocumentDB\",\"rowversion\":\"xgsj2\",\"tables\":"
        			+ "[{\"table\":\"da_jbxx\",\"xgsj\":\"0000000000000000\"},{\"table\":\"da_jl\",\"xgsj\":\"0000000000000000\"},"
        			+ "{\"table\":\"da_qklj\",\"xgsj\":\"0000000000000000\"},{\"table\":\"da_shgx\",\"xgsj\":\"0000000000000000\"},"
        			+ "{\"table\":\"da_tzzb\",\"xgsj\":\"0000000000000000\"},{\"table\":\"da_tszb\",\"join\":[\"da_tsbc\"],\"key\":\"da_tszb.bh=da_tsbc.bh\",\"xgsj\":\"0000000000000000\"},"
        			+ "{\"table\":\"da_clzl\",\"xgsj\":\"0000000000000000\"},{\"table\":\"da_crj\",\"xgsj\":\"0000000000000000\"},"
        			+ "{\"table\":\"da_swdj\",\"xgsj\":\"0000000000000000\"},{\"table\":\"da_tc\",\"xgsj\":\"0000000000000000\"},"
        			+ "{\"table\":\"da_zm\",\"xgsj\":\"0000000000000000\"},{\"table\":\"yzjc\",\"xgsj\":\"0000000000000000\"},"
        			+ "{\"table\":\"xfzb\",\"xgsj\":\"0000000000000000\"},{\"table\":\"djbd\",\"xgsj\":\"0000000000000000\"},"
        			+ "{\"table\":\"db\",\"xgsj\":\"0000000000000000\"},{\"table\":\"nwfg\",\"xgsj\":\"0000000000000000\"},{\"table\":\"hjdd\",\"xgsj\":\"0000000000000000\"},"
        			+ "{\"table\":\"hjbd\",\"xgsj\":\"0000000000000000\"},{\"table\":\"jxcd\",\"xgsj\":\"0000000000000000\"},{\"table\":\"st\",\"xgsj\":\"0000000000000000\"},"
        			+ "{\"table\":\"jy_rzfpbd\",\"xgsj\":\"0000000000000000\"},{\"table\":\"jwbw\",\"xgsj\":\"0000000000000000\"},"
        			+ "{\"table\":\"bwxb\",\"xgsj\":\"0000000000000000\"},{\"table\":\"tt\",\"xgsj\":\"0000000000000000\"},{\"table\":\"lbc\",\"xgsj\":\"0000000000000000\"},"
        			+ "{\"table\":\"ss\",\"join\":[\"ssfb\"],\"key\":\"ss.ssid=ssfb.ssid\",\"xgsj\":\"0000000000000000\"},"
        			+ "{\"table\":\"ks\",\"join\":[\"ksfb\"],\"key\":\"ks.bh=ksfb.bh AND ks.ksrq=ksfb.ksrq\",\"xgsj\":\"0000000000000000\"},"
        			+ "{\"table\":\"jfjjzb\",\"xgsj\":\"0000000000000000\"},{\"table\":\"tbjd\",\"xgsj\":\"0000000000000000\"},{\"table\":\"gzjs\",\"xgsj\":\"0000000000000000\"},"
        			+ "{\"table\":\"qjfj\",\"xgsj\":\"0000000000000000\"},{\"table\":\"yjdj\",\"xgsj\":\"0000000000000000\"},{\"table\":\"sg\",\"xgsj\":\"0000000000000000\"},"
        			+ "{\"table\":\"em_zb\",\"xgsj\":\"0000000000000000\"},{\"table\":\"em_qk\",\"join\":[\"em_zb\"],\"key\":\"em_qk.bh=em_zb.bh\",\"xgsj\":\"0000000000000000\"},"
        			+ "{\"table\":\"em_jd\",\"join\":[\"em_zb\"],\"key\":\"em_jd.bh=em_zb.bh\",\"xgsj\":\"0000000000000000\"},"
        			+ "{\"table\":\"em_jc\",\"join\":[\"em_zb\"],\"key\":\"em_jc.bh=em_zb.bh\",\"xgsj\":\"0000000000000000\"},"
        			+ "{\"table\":\"em_sy\",\"join\":[\"em_zb\"],\"key\":\"em_sy.bh=em_zb.bh\",\"xgsj\":\"0000000000000000\"},"
        			+ "{\"table\":\"fpa_zacy\",\"join\":[\"fpa_zb\",\"fpa_swry\"],\"key\":\"fpa_zacy.ah=fpa_zb.ah AND fpa_zacy.ah=fpa_swry.ah\",\"xgsj\":\"0000000000000000\"},"
        			+ "{\"table\":\"yma_zacy\",\"join\":[\"yma_zb\"],\"key\":\"yma_zacy.ah=yma_zb.ah\",\"xgsj\":\"0000000000000000\"},"
        			+ "{\"table\":\"wjp_bc\",\"join\":[\"wjp_zb\"],\"key\":\"wjp_bc.wjpid=wjp_zb.wjpid\",\"xgsj\":\"0000000000000000\"},"
        			+ "{\"table\":\"wyld_ry\",\"join\":[\"wyld_zb\"],\"key\":\"wyld_ry.wydid=wyld_zb.wydid\",\"xgsj\":\"0000000000000000\"},"
        			+ "{\"table\":\"hj\",\"join\":[\"hj_fb\"],\"key\":\"hj.hjid=hj_fb.hjid\",\"xgsj\":\"0000000000000000\"},{\"table\":\"khjf\",\"xgsj\":\"0000000000000000\"},"
        			+ "{\"table\":\"khjf_sd\",\"xgsj\":\"0000000000000000\"},{\"table\":\"khf\",\"xgsj\":\"0000000000000000\"},{\"table\":\"thdj\",\"xgsj\":\"0000000000000000\"},"
        			+ "{\"table\":\"wp_bgzb\",\"join\":[\"wp_bgbc\"],\"key\":\"wp_bgzb.bh=wp_bgbc.bh AND wp_bgzb.djrq=wp_bgbc.djrq\",\"xgsj\":\"0000000000000000\"},"
        			+ "{\"table\":\"wwzk\",\"xgsj\":\"0000000000000000\"},{\"table\":\"wwjc\",\"xgsj\":\"0000000000000000\"},"
        			+ "{\"table\":\"wwbx\",\"join\":[\"wwzk\"],\"key\":\"wwbx.bh=wwzk.bh AND wwbx.pzrq=wwzk.pzrq\",\"xgsj\":\"0000000000000000\"},{\"table\":\"sndd\",\"xgsj\":\"0000000000000000\"}]}]}";
		}else if (tableId.endsWith("documentfile")) {
			json = "{\"databases\":[{\"db\":\"DocumentFiles\",\"rowversion\":\"filetime\",\"tables\":[{\"table\":\"c://罪犯媒体/像片\",\"xgsj\":\"0000000000000000\"}]}]}";
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
}
