package com.cloudbeaver.hdfsHttpProxy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.security.Key;
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

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.mindrot.jbcrypt.BCrypt;

import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.client.common.HdfsHelper;
import com.cloudbeaver.client.common.SqlHelper;
import com.cloudbeaver.client.dbbean.DatabaseBean;
import net.sf.json.JSONObject;

@WebServlet("/getFileInfo")
public class GetFileInfoServlet extends HttpServlet{

	private static Logger logger = Logger.getLogger(GetFileInfoServlet.class);
	private static String hdfsPrefix = "hdfs://localhost:9000/test/";

	@Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException{
    	BufferedReader br = new BufferedReader(new InputStreamReader((ServletInputStream) req.getInputStream()));
    	StringBuilder sb = new StringBuilder();
    	String line = null;
    	int ERROR_CODE = 0;
        while ((line = br.readLine()) != null) {
            sb.append(line);
        }
        String json = sb.toString();
        JSONObject jsonObject = JSONObject.fromObject(json);
        String fileName = jsonObject.get("fileName").toString();
        String userName = jsonObject.get("userName").toString();
        String passWd = jsonObject.get("passWd").toString();
        String tableUrl = jsonObject.get("tableUrl").toString();
        logger.info("TableUrl = " + tableUrl);
        String tableId = tableUrl.substring(tableUrl.lastIndexOf("/")+1);

        DatabaseBean dbBean = new DatabaseBean();
        dbBean.setDb("beaver_web_development");
        dbBean.setDbUserName("beaver");
        dbBean.setDbPassword("123456");
        dbBean.setType("postgresDB");
        dbBean.setDbUrl("jdbc:postgresql://localhost/beaver_web_development");
        String uploadKey = null;
    	try {
			Connection conn = SqlHelper.getDBConnectionNoRetry(dbBean);
			Statement st = conn.createStatement();
			String sql = "select \"email\", \"password\", \"ownerId\",\"linkId\",\"uploadKey\" "
					+ "from \"Tables\" t, \"Files\" f, \"Users\" u "
					+ "where t.id = f.\"linkId\" and u.id = f.\"ownerId\" and t.id = '" + tableId + "';";
			logger.info("sql = " + sql);
			ResultSet rs = st.executeQuery(sql);
			String password = null;
			String email = null;
			while (rs.next()){
				password = rs.getString("password");
				email = rs.getString("email");
				uploadKey = rs.getString("uploadKey");
				if(email.equals(userName) && BCrypt.checkpw(passWd, password)){
					logger.info("user matches");
				}
				else{
					throw new IOException("user does not match");
				}
				
			}
		} catch (SQLException | ClassNotFoundException e1) {
			logger.error("connect to database failed!");
			BeaverUtils.printLogExceptionAndSleep(e1, "connect to database failed!", 5000);
			ERROR_CODE = 5;
		}

    	byte [] token = null;
    	jsonObject = new JSONObject();
    	long length = Integer.MIN_VALUE;
    	if(ERROR_CODE == 0){
    		length = HdfsHelper.getFileLength(hdfsPrefix + fileName);
    		JSONObject tokenJson = new JSONObject();
    		tokenJson.put("uploadKey", uploadKey);
    		tokenJson.put("userName", userName);
    		tokenJson.put("passWd", passWd);
    		tokenJson.put("position", hdfsPrefix + fileName);
    		logger.info("before encryption, token = " + tokenJson.toString());
    		token = BeaverUtils.encryptAes(tokenJson.toString().getBytes(), "123456");
    		logger.info("after encryption, token = " + token);
    	}
    	jsonObject.put("fileName", fileName);
    	jsonObject.put("length", length);
    	jsonObject.put("errorCode", ERROR_CODE);
    	jsonObject.put("token", Base64.encodeBase64String(token));

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