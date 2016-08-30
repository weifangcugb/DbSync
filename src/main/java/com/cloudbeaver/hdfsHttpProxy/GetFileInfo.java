package com.cloudbeaver.hdfsHttpProxy;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;
import com.cloudbeaver.client.common.BeaverUtils;
import net.sf.json.JSONObject;

@WebServlet("/fileinfo")
public class GetFileInfo extends HttpServlet{
	private static Logger logger = Logger.getLogger(GetFileInfo.class);

	@Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException{
    	String fileName = req.getParameter("fileName");
    	long length = HdfsHelper.getFileLength(fileName);
    	writeOffset(resp, fileName, length);
    }

    private void writeOffset(HttpServletResponse resp, String fileName, long offset){
    	FileInfoBean fBean = new FileInfoBean();
    	fBean.offset = offset;

    	Map<String, FileInfoBean> fileInfoMap = new HashMap<>();
    	fileInfoMap.put(fileName, fBean);

    	JSONObject json = JSONObject.fromObject(fileInfoMap);

    	resp.setHeader("Content-type", "text/html;charset=UTF-8");
    	PrintWriter pw;
		try {
			pw = resp.getWriter();
			pw.write(json.toString());
	        pw.flush();
	        pw.close();
		} catch (IOException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("get file info failed!");
		}
    }

    protected void doPost2(HttpServletRequest req, HttpServletResponse resp){
    	Map<String, FileInfoBean> fileInfoMap = HdfsProxyServlet.getFileInfo();
    	JSONObject json = JSONObject.fromObject(fileInfoMap);
//    	System.out.println(json.toString());

    	resp.setHeader("Content-type", "text/html;charset=UTF-8");
    	PrintWriter pw;
		try {
			pw = resp.getWriter();
			pw.write(json.toString());
	        pw.flush();
	        pw.close();
		} catch (IOException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("get file info failed!");
		}
    }

	public static void main(String[] args) {
	}
}
