package com.cloudbeaver.hdfsHttpProxy;

import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;
import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.client.common.HdfsHelper;

import net.sf.json.JSONObject;

@WebServlet("/fileinfo")
public class GetFileInfoServlet extends HttpServlet{

	private static Logger logger = Logger.getLogger(GetFileInfoServlet.class);

	@Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException{
    	String fileName = req.getParameter("fileName");
    	long length = HdfsHelper.getFileLength(fileName);    	
    	JSONObject jsonObject = new JSONObject();
    	jsonObject.put("file", fileName);
    	jsonObject.put("length", length);

    	resp.setHeader("Content-type", "text/html;charset=UTF-8");
    	PrintWriter pw;
		try {
			pw = resp.getWriter();
			pw.write(jsonObject.toString());
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
