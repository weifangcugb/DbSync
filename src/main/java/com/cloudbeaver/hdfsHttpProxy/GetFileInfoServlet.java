package com.cloudbeaver.hdfsHttpProxy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;
import org.mindrot.jbcrypt.BCrypt;

import com.cloudbeaver.client.common.BeaverFatalException;
import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.client.common.HdfsHelper;
import com.cloudbeaver.client.common.SqlHelper;
import com.cloudbeaver.client.dbbean.DatabaseBean;
import net.sf.json.JSONObject;

@WebServlet("/getFileInfo")
public class GetFileInfoServlet extends HttpServlet{

	private static Logger logger = Logger.getLogger(GetFileInfoServlet.class);

	@Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException{
    	BufferedReader br = new BufferedReader(new InputStreamReader((ServletInputStream) req.getInputStream()));
    	StringBuilder sb = new StringBuilder();
    	String line = null;
        while ((line = br.readLine()) != null) {
            sb.append(line);
        }
        String json = sb.toString();
        JSONObject jsonObject = JSONObject.fromObject(json);
        String fileName = jsonObject.get("fileName").toString();
        String userName = jsonObject.get("userName").toString();
        String passWd = jsonObject.get("passWd").toString();
        String tableUrl = jsonObject.get("tableUrl").toString();

        DatabaseBean dbBean = new DatabaseBean();
        dbBean.setDb("beaver_web_development");
        dbBean.setDbUserName("beaver");
        dbBean.setDbPassword("123456");
        dbBean.setType("postgresDB");
        dbBean.setDbUrl("jdbc:postgresql://localhost/beaver_web_development");
    	try {
			Connection conn = SqlHelper.getConnKeepTrying(dbBean);
			Statement st = conn.createStatement();
			String sql = "select * from \"Users\" where email = '" + userName + "';";
			System.out.println(sql);
			ResultSet rs = st.executeQuery(sql);
			String password = null;
			while (rs.next()){
				password = rs.getString("password");
				logger.info("password = " + password);
				if(BCrypt.checkpw(passWd, password)){
					logger.info("user matches");
				}
				else{
					throw new IOException("user does not match");
				}
			}
		} catch (BeaverFatalException | SQLException e1) {
			logger.error("connect to database failed!");
			BeaverUtils.printLogExceptionAndSleep(e1, "connect to database failed!", 5000);
			return ;
		}

    	long length = HdfsHelper.getFileLength(fileName);
    	jsonObject = new JSONObject();
    	jsonObject.put("fileName", fileName);
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