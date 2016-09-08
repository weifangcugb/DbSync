package com.cloudbeaver.hdfsHttpProxy;

import java.io.DataInputStream;
import java.io.IOException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.crypto.KeyGenerator;
import javax.crypto.spec.SecretKeySpec;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.client.common.HdfsHelper;

import net.sf.json.JSONObject;

@WebServlet("/uploadData")
public class UploadFileServlet extends HttpServlet{
	private static Logger logger = Logger.getLogger(UploadFileServlet.class);
	public static int BUFFER_SIZE;

	@Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	doPost(req, resp);
    }

    public static int getBUFFER_SIZE() {
		return BUFFER_SIZE;
	}

	public static void setBUFFER_SIZE(int bUFFER_SIZE) {
		BUFFER_SIZE = bUFFER_SIZE;
	}

	@Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException{
		setBUFFER_SIZE(HdfsProxyServer.getConf().getBufferSize());
    	logger.info("start upload data to HDFS");
    	String filename = req.getParameter("fileName");
    	String token = req.getParameter("token");
    	logger.info("before decryption, token = " + token);
    	String tokenJson = new String(BeaverUtils.decryptAes(Base64.decodeBase64(token), HdfsProxyServer.SERVER_PASSWORD));
    	logger.info("after decryption, token = " + tokenJson);
    	JSONObject jObject = JSONObject.fromObject(tokenJson);
    	String position = jObject.getString("position");

    	int readNumTillNow = 0, len = 0;
    	byte[] buffer = new byte[BUFFER_SIZE];
    	DataInputStream dataInputStream = new DataInputStream(req.getInputStream());
    	while((len = dataInputStream.read(buffer, readNumTillNow, BUFFER_SIZE - readNumTillNow)) != -1){
    		logger.info("len = " + len);
			readNumTillNow += len;
			if(readNumTillNow == BUFFER_SIZE){
				HdfsHelper.writeFile(position, buffer, readNumTillNow);
				logger.info("upload data, file:" + filename + " size:" + readNumTillNow);

//				clear buffer and read next block
				readNumTillNow = 0;
				BeaverUtils.clearByteArray(buffer);
			}
		}
    	logger.info("len = " + len);

//    	read to the end of the file
    	if (readNumTillNow > 0) {
    		logger.info("readNumTillNow = " + readNumTillNow);
    		logger.info("Start writing data to HDFS");
    		HdfsHelper.writeFile(position, buffer, readNumTillNow);
//    		while(true){
//    			System.out.println("writing " + System.currentTimeMillis());
//    			BeaverUtils.sleep(1000);
//    		}
    		logger.info("Finish writing data to HDFS");
		}
    }

	public Key getKey(String keyContent) throws NoSuchAlgorithmException {
    	KeyGenerator kgen = KeyGenerator.getInstance("AES");  
        kgen.init(128, new SecureRandom(keyContent.getBytes())); 
        return new SecretKeySpec(kgen.generateKey().getEncoded(), "AES");
	}
}