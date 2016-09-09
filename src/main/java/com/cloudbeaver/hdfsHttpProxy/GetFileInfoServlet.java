package com.cloudbeaver.hdfsHttpProxy;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
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
        JSONObject reqJson = JSONObject.fromObject(IOUtils.toString(req.getInputStream()));
        String fileName = reqJson.getString("fileName");
        String userName = reqJson.getString("userName");
        String passWd = reqJson.getString("passWd");
        String tableId = reqJson.getString("tableId");

        JSONObject respJson = new JSONObject();
		DatabaseBean dbBean = new DatabaseBean();
        dbBean.setDb(HdfsProxyServer.conf.getDbName());
        dbBean.setDbUserName(HdfsProxyServer.conf.getDbUser());
        dbBean.setDbPassword(HdfsProxyServer.conf.getDbPass());
        dbBean.setType(HdfsProxyServer.conf.getDbType());
        dbBean.setDbUrl(HdfsProxyServer.conf.getDbUrl());

        Connection conn = null;
    	try{
    		conn = SqlHelper.getDBConnectionNoRetry(dbBean);
			Statement st = conn.createStatement();
			String sql = "select \"email\", \"password\", \"ownerId\",\"linkId\",\"uploadKey\" "
					+ "from \"Tables\" t, \"Files\" f, \"Users\" u "
					+ "where t.id = f.\"linkId\" and u.id = f.\"ownerId\" and t.id = '" + tableId + "';";
			ResultSet rs = st.executeQuery(sql);
			String password = null;
			String email = null;
			if(rs.next()){
				password = rs.getString("password");
				email = rs.getString("email");
				String uploadKey = rs.getString("uploadKey");
				if( email.equals(userName) && BCrypt.checkpw(passWd, password) ){
		    		long length = HdfsHelper.getFileLength(hdfsPrefix + fileName);

		    		JSONObject tokenJson = new JSONObject();
		    		tokenJson.put("uploadKey", uploadKey);
		    		tokenJson.put("userName", userName);
		    		tokenJson.put("passWd", passWd);
		    		tokenJson.put("position", hdfsPrefix + fileName);
		    		tokenJson.put("offset", length);
		    		logger.info("before encryption, token = " + tokenJson);
		    		byte[] token = BeaverUtils.encryptAes(tokenJson.toString().getBytes(), HdfsProxyServer.SERVER_PASSWORD );
		    		logger.info("after encryption, token = " + token);

		    		respJson.put("errorCode", BeaverUtils.ErrCode.OK.ordinal());
		    		respJson.put("fileName", fileName);
		    		respJson.put("offset", length);
		    		respJson.put("token", URLEncoder.encode(Base64.encodeBase64String(token)));
				} else {
					respJson.put("errorCode", BeaverUtils.ErrCode.PASS_CHECK_ERROR.ordinal());
				}
			} else {
				respJson.put("errorCode", BeaverUtils.ErrCode.USER_NOT_EXIST.ordinal());
			}
		} catch(SQLException  e) {
			SqlHelper.closeConnection(conn, dbBean);
			respJson.put("errorCode", BeaverUtils.ErrCode.SQL_ERROR.ordinal());
			BeaverUtils.printLogExceptionWithoutSleep(e, "connect to database failed");
		} catch(ClassNotFoundException e) {
			respJson.put("errorCode", BeaverUtils.ErrCode.OTHER_ERROR.ordinal());
			BeaverUtils.printLogExceptionWithoutSleep(e, "class not fount, server fatal error");
		}

    	resp.setHeader("Content-type", "text/html;charset=UTF-8");
    	PrintWriter pw;
		try {
			pw = resp.getWriter();
			pw.write(respJson.toString());
	        pw.flush();
	        pw.close();
		} catch (IOException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("get file info failed!");
		}
    }
}