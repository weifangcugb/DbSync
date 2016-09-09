package com.cloudbeaver.hdfsHttpProxy;

import java.io.DataInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import javax.servlet.ServletException;
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

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

@WebServlet("/uploadData")
public class UploadFileServlet extends HttpServlet{
	private static Logger logger = Logger.getLogger(UploadFileServlet.class);
	public static int BUFFER_SIZE = HdfsProxyServer.conf.getBufferSize();

	@Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	doPost(req, resp);
    }

	@Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException{
    	String token = req.getParameter("token");
    	String tokenJson = new String(BeaverUtils.decryptAes(Base64.decodeBase64(token), HdfsProxyServer.SERVER_PASSWORD));
    	JSONObject jObject = JSONObject.fromObject(tokenJson);
    	BeaverUtils.ErrCode errCode = veryfiToken(jObject);
    	if(errCode == BeaverUtils.ErrCode.OK){
        	String location = jObject.getString("location");
        	int readNumTillNow = 0, len = 0;
        	byte[] buffer = new byte[BUFFER_SIZE];
        	DataInputStream dataInputStream = new DataInputStream(req.getInputStream());
        	while((len = dataInputStream.read(buffer, readNumTillNow, BUFFER_SIZE - readNumTillNow)) != -1){
    			readNumTillNow += len;
    			if(readNumTillNow == BUFFER_SIZE){
    				HdfsHelper.writeFile(location, buffer, readNumTillNow);

//    				clear buffer and read next block
    				readNumTillNow = 0;
    				BeaverUtils.clearByteArray(buffer);
    			}
    		}

//        	read to the end of the file
        	if (readNumTillNow > 0) {
        		HdfsHelper.writeFile(location, buffer, readNumTillNow);
    		}
    	}else{
    		throw new IOException("verify token error, errCode:" + errCode.getErrMsg());
    	}
    }

	private BeaverUtils.ErrCode veryfiToken(JSONObject tokenOject){
		DatabaseBean dbBean = HdfsProxyServer.getDataBaseBeanFromConf();
    	try (Connection conn = SqlHelper.getDBConnection(dbBean)) {
    		String userName = tokenOject.getString("username");
    		String password = tokenOject.getString("password");
    		long requestTime = tokenOject.getLong("requestTime");
    		String tableId = tokenOject.getString("tableId");
    		String uploadKey = tokenOject.getString("uploadKey");

			JSONArray resultArray = SqlHelper.execSimpleQuery(HdfsProxyServer.getSQLFromTableId(tableId), dbBean, conn);
			if(resultArray.size() > 0){
				for(int i = 0; i < resultArray.size(); i++){
					JSONObject jObject = resultArray.getJSONObject(i);
					if ( jObject.getString("email").equals(userName) && BCrypt.checkpw(jObject.getString("password"), password)
						&& jObject.getString("uploadKey").equals(uploadKey) && System.currentTimeMillis() > (requestTime + HdfsProxyServer.TOKEN_EXPIRE_INTERVAL)) {
						return BeaverUtils.ErrCode.OK;
					}
				}
			}

			return BeaverUtils.ErrCode.PASS_CHECK_ERROR;
    	} catch(SQLException | ClassNotFoundException  e) {
			BeaverUtils.printLogExceptionWithoutSleep(e, "connect to database failed");
			return BeaverUtils.ErrCode.SQL_ERROR;
		}
	}
}