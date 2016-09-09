package com.cloudbeaver.hdfsHttpProxy;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.SQLException;
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

import net.sf.json.JSONArray;
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
        String userName = reqJson.getString(HdfsProxyServer.EMAIL);
        String password = reqJson.getString(HdfsProxyServer.PASSWORD);
        String tableId = reqJson.getString(HdfsProxyServer.TABLEID);

        JSONObject respJson = new JSONObject();
        DatabaseBean dbBean = HdfsProxyServer.getDataBaseBeanFromConf();
    	try (Connection conn = SqlHelper.getDBConnection(dbBean)) {
			JSONArray resultArray = SqlHelper.execSimpleQuery(HdfsProxyServer.getSQLFromTableId(tableId), dbBean, conn);
			if(resultArray.size() > 0){
				for(int i = 0; i < resultArray.size(); i++){
					JSONObject jObject = resultArray.getJSONObject(i);
					if( jObject.getString(HdfsProxyServer.EMAIL).equals(userName) && BCrypt.checkpw(password, jObject.getString(HdfsProxyServer.PASSWORD)) ){
			    		long length = HdfsHelper.getFileLength(hdfsPrefix + HdfsHelper.getRealPathWithTableId(tableId));
			    		if (length == -1) {
							length = 0;
						}

			    		JSONObject tokenJson = new JSONObject();
			    		tokenJson.put(HdfsProxyServer.TABLEID, tableId);
			    		tokenJson.put(HdfsProxyServer.UPLOADKEY, jObject.getString(HdfsProxyServer.UPLOADKEY));
			    		tokenJson.put(HdfsProxyServer.EMAIL, userName);
			    		tokenJson.put(HdfsProxyServer.PASSWORD, password);
			    		tokenJson.put(HdfsProxyServer.LOCATION, hdfsPrefix + HdfsHelper.getRealPathWithTableId(tableId));
			    		tokenJson.put(HdfsProxyServer.OFFSET, length);
			    		tokenJson.put(HdfsProxyServer.REQUESTTIME, System.currentTimeMillis());

			    		byte[] token = BeaverUtils.encryptAes(tokenJson.toString().getBytes(), HdfsProxyServer.SERVER_PASSWORD );
			    		respJson.put(HdfsProxyServer.TOKEN, URLEncoder.encode(Base64.encodeBase64String(token), BeaverUtils.DEFAULT_CHARSET));
			    		respJson.put(HdfsProxyServer.ERRORCODE, BeaverUtils.ErrCode.OK.ordinal());
			    		respJson.put(HdfsProxyServer.OFFSET, length);
					} else {
						respJson.put(HdfsProxyServer.ERRORCODE, BeaverUtils.ErrCode.PASS_CHECK_ERROR.ordinal());
					}
				}
			} else {
				respJson.put(HdfsProxyServer.ERRORCODE, BeaverUtils.ErrCode.USER_NOT_EXIST.ordinal());
			}
		} catch(SQLException  e) {
			respJson.put(HdfsProxyServer.ERRORCODE, BeaverUtils.ErrCode.SQL_ERROR.ordinal());
			BeaverUtils.printLogExceptionWithoutSleep(e, "connect to database failed");
		} catch(ClassNotFoundException | SecurityException e) {
			respJson.put(HdfsProxyServer.ERRORCODE, BeaverUtils.ErrCode.OTHER_ERROR.ordinal());
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