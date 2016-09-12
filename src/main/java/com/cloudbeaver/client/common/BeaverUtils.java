package com.cloudbeaver.client.common;

import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.security.Key;
import java.security.KeyManagementException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import javax.imageio.ImageIO;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

//import com.sun.image.codec.jpeg.JPEGCodec;
//import com.sun.image.codec.jpeg.JPEGImageEncoder;

class beaverTrustManager implements X509TrustManager{
	@Override 
	public X509Certificate[] getAcceptedIssuers() {
		return null; 
	}
	@Override 
	public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException { 
	}
	@Override 
	public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException { 
	}
}

public class BeaverUtils {
	private static Logger logger = Logger.getLogger(BeaverUtils.class);

	public enum ErrCode{
		OK("all things ok"),
		SQL_ERROR("can't execute sql query"),
		USER_NOT_EXIST("user doesn't exist"),
		PASS_CHECK_ERROR("password is wrong"),
		OTHER_ERROR("other error");

		private String errMsg;
		ErrCode(String errMsg) {
			this.errMsg = errMsg;
		}
		public String getErrMsg(){
			return errMsg;
		}
	}

	public static String DEFAULT_CHARSET = "utf-8";

	public static boolean DEBUG_MODE = true;

	private static SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");

	private static final PostStringUploader postStringUploader = new PostStringUploader();
	private static final PostFileUploader postFileUploader = new PostFileUploader();

	public static void PrintStackTrace(Exception e) {
		if (DEBUG_MODE) {
			e.printStackTrace();
		}
	}

	public static String doGet(String urlString) throws IOException {
		BufferedReader br = null;
		try {
			if (urlString.indexOf("http://") == -1) {
				urlString = "http://" + urlString;
			}

			URL url = new URL(urlString);

			HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
	        urlConnection.setRequestMethod("GET");
	        urlConnection.connect();

	        br = new BufferedReader(new InputStreamReader(urlConnection.getInputStream(), "utf-8"));
	        StringBuilder sb = new StringBuilder();
	        String tmp = "";
	        while((tmp = br.readLine()) != null){
	        	sb.append(tmp);
	        }
	        if (urlConnection.getResponseCode() == 200) {
				return sb.toString();
			}else {
				throw new IOException("http server return not SC_OK, responseCode:" + urlConnection.getResponseCode());
			}
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					logger.error("close url reader error, msg:" + e.getMessage() + " url:" + urlString);
				}
			}
		}
	}

	public static void clearByteArray(byte[] buffer){
		for (int i = 0; i < buffer.length; i++) {
			buffer[i] = 0;
		}
	}

	public static void sleep(long sleepTime) {
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            logger.debug("sleep interrupted, msg:" + e.getMessage());
        }
	}

	public static StringBuilder doPost(String webUrl, Map<String, String> paraMap, String contentType) throws IOException {
		StringBuilder sb = new StringBuilder();
		Set<String> keySet = paraMap.keySet();
		boolean first = true;
		for (String key : keySet) {
			if (!first) {
				sb.append('&');
			}
			first = false;
			sb.append(key).append('=').append(paraMap.get(key));
		}
		return doPost(webUrl, sb.toString(), 0, contentType, postStringUploader, false);
	}

	public static String doPost(String urlString, String flumeJson) throws IOException {
		return doPost(urlString, flumeJson, false).toString();
	}

	public static String doPost(String urlString, String flumeJson, boolean useHttps) throws IOException {
		return doPost(urlString, flumeJson, 0, "application/json", postStringUploader, useHttps).toString();
	}

	public static String doPostBigFile(String urlString, String fileName, long seekPos) throws IOException {
		return doPost(urlString, fileName, seekPos, "application/octet-stream", postFileUploader, false).toString();
	}

	private static StringBuilder doPost(String urlString, String content, long startIdx, String contentType, PostUploader postUploader, boolean useHttps) throws IOException {
		BufferedReader br = null;
		StringBuilder sb = new StringBuilder();
		try {
			if (!(urlString.startsWith("https://") || urlString.startsWith("http://"))) {
				if (useHttps) {
					urlString = "https://" + urlString;
				} else {
					urlString = "http://" + urlString;
				}
			}

			URL url = new URL(urlString);

			URLConnection urlConnection = url.openConnection();
			urlConnection.setDoInput(true);
			urlConnection.setConnectTimeout(20000);
	        urlConnection.setRequestProperty("Content-Type", contentType + ";charset=utf-8");//text/plain
	        logger.debug(urlConnection.getRequestProperty("Content-Type"));

			if (useHttps) {
				HttpsURLConnection httpsURLConnection = (HttpsURLConnection) urlConnection;

				try{
					SSLContext sslcontext = SSLContext.getInstance("TLS"); 
					sslcontext.init(null, new TrustManager[]{new beaverTrustManager()}, null);
					httpsURLConnection.setSSLSocketFactory(sslcontext.getSocketFactory());
					httpsURLConnection.setHostnameVerifier(new HostnameVerifier() {
			            public boolean verify(String hostname, SSLSession session) {
			              return true;
			            }
			        });
				} catch(NoSuchAlgorithmException | KeyManagementException e) {
					PrintStackTrace(e);
					throw new IOException(e.getMessage());
				}

		        httpsURLConnection.setRequestMethod("POST");
		        postUploader.setUrlConnectionProperty(httpsURLConnection, content);
			} else {
				HttpURLConnection httpURLConnection = (HttpURLConnection) urlConnection;

				httpURLConnection.setRequestMethod("POST");
		        postUploader.setUrlConnectionProperty(httpURLConnection, content);
			}

	        if (content != null) {
	        	urlConnection.setDoOutput(true);
        		try( DataOutputStream out = new DataOutputStream(urlConnection.getOutputStream()) ){
		        	postUploader.upload(out, content, startIdx);
		        }
			}

	        BufferedReader in = new BufferedReader(new InputStreamReader(urlConnection.getInputStream()));
	        String line = "";
	        while ((line = in.readLine()) != null) {
	            sb.append(line);
	        }

	        int responseCode = -1;
	        if (useHttps) {
				responseCode = ((HttpsURLConnection)urlConnection).getResponseCode();
			} else {
				responseCode = ((HttpURLConnection)urlConnection).getResponseCode();
			}
	        logger.debug("Got reply message from web, server:" + urlString + " responseCode:" + responseCode + " reply:" + sb.toString());
		}finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					logger.error("close url reader error, msg:" + e.getMessage() + " url:" + urlString);
				}
			}
		}
        return sb;
	}

	/*
	 * load a config file to a string=>string map
	 * will clear the map first
	 */
    public static Map<String, String> loadConfig(String confFileName) throws FileNotFoundException, IOException{
    	Map<String, String> conf = new HashMap<String, String>();

        Properties pps = new Properties();

//        pps.load(BeaverUtils.class.getClassLoader().getResourceAsStream(confFileName));
        logger.info("conf_name:" + new File(confFileName).getAbsolutePath());
        pps.load(new FileInputStream(new File(confFileName)));
        Enumeration<?> enum1 = pps.propertyNames();
        while(enum1.hasMoreElements()) {
            String strKey = (String) enum1.nextElement();
            String strValue = pps.getProperty(strKey);
            logger.debug(strKey.trim() + "=" + strValue.trim());
            conf.put(strKey.trim(), strValue.trim());
        }

        if (conf.isEmpty()) {
			logger.warn("config file is empty, please notice this");
		}
        return conf;
    }

	public static String gzipAndbase64(String data) throws IOException {
//		version 1: just replase "
//		data = data.replaceAll("\"", "\\\\\"");

//		version 2: gzip and base64 encode
//		ByteArrayOutputStream bout = new ByteArrayOutputStream();
//		GZIPOutputStream gout = new GZIPOutputStream(bout);
//		gout.write(data.getBytes(Charset.forName(DEFAULT_CHARSET)));
//		gout.close();
//		return Base64.encodeBase64String(bout.toByteArray());

//		version 3: base64 encode only
		return Base64.encodeBase64String(data.getBytes(DEFAULT_CHARSET));
	}

	public static String compressAndFormatFlumeHttp(String data) throws IOException {
		return "[{ \"headers\" : {}, \"body\" : \"" + gzipAndbase64(data) + "\" }]";
	}

	public static byte[] decompress(byte[] base64) throws IOException {
//		version 1:
//		return new String(base64, DEFAULT_CHARSET);

//		version 2: base64 decode and gunzip
//		return UnGzip(Base64.decodeBase64(base64));

//		version 3: base64 decode only
		return Base64.decodeBase64(base64);
	}

	private static byte[] UnGzip(byte[] data) throws IOException {
		GZIPInputStream gzipIn = new GZIPInputStream(new ByteArrayInputStream(data));
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		byte[] buffer = new byte[1024];
		int len = 0;
		while((len = gzipIn.read(buffer)) != -1){
			bout.write(buffer, 0, len);
		}
		return bout.toByteArray();
	}

	public static boolean isHttpServerInternalError(String message) {
		return message.indexOf("HTTP response code: 500") != -1;
	}

	public static long hexTolong(String miniChangeTime) {
		return Long.parseLong(miniChangeTime, 16);
	}

	public static String longToHex(long miniChangeTime) {
		String hex = Long.toHexString(miniChangeTime);
		int len = hex.length();
		for (int i = 0; i < 16 - len; i++) {
			hex = '0' + hex;
		}
		return hex;
	}

	public static byte[] resizePic(File file, int oriFileSize, int newFileSize) throws IOException {
		double radio = newFileSize * 1.0 / oriFileSize;
		logger.info("resize pic, radio:" + radio);

		Image img = ImageIO.read(file);
		int width = (int)(img.getWidth(null) * radio);
		int height = (int)(img.getHeight(null) * radio);

		BufferedImage bImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
		bImage.getGraphics().drawImage(img, 0, 0, width, height, null);

		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		ImageIO.write(bImage, "jpeg", bout);
//		JPEGImageEncoder jpegEncoder = JPEGCodec.createJPEGEncoder(bout);
//		jpegEncoder.encode(bImage);
		return bout.toByteArray();
	}

	public static boolean fileIsPics(String fileName) {
		return fileName.endsWith("jpg") || fileName.endsWith("jpeg") || fileName.endsWith("bmp") || fileName.endsWith("png");
	}

	public static void printLogExceptionAndSleep(Exception e, String msgPrefix, int sleepTime) {
		printLogExceptionWithoutSleep(e, msgPrefix);
		BeaverUtils.sleep(sleepTime);
	}

	public static void printLogExceptionWithoutSleep(Exception e, String msgPrefix) {
		BeaverUtils.PrintStackTrace(e);
		logger.error(msgPrefix + ", msg:" + e.getMessage());
	}

	public static String getRequestSign(Map<String, String> paraMap, String appSecret) throws NoSuchAlgorithmException {
//		the keys in the paraMap should be ordered
		List<String> paramList = new ArrayList<>(paraMap.size());
		paramList.addAll(paraMap.keySet());
		Collections.sort(paramList);
		StringBuilder sb = new StringBuilder();
		sb.append(appSecret);
		for (String key : paramList) {
			sb.append(key).append(paraMap.get(key));
		}
		sb.append(appSecret);
		MessageDigest md = MessageDigest.getInstance("md5");
		return toHexStringUpperCase(md.digest(sb.toString().getBytes()));
	}

	private static String toHexStringUpperCase(byte[] digest) {
		return toHexString(digest).toUpperCase();
	}

	private static String toHexString(byte bytes[]) {
        StringBuilder hs = new StringBuilder();
        String stmp = "";
        for (int n = 0; n < bytes.length; n++) {
            stmp = Integer.toHexString(bytes[n] & 0xff);
            if (stmp.length() == 1)
                hs.append("0").append(stmp);
            else
                hs.append(stmp);
        }
 
        return hs.toString();
    }

	public static String timestampToDateString(String timestamp) {
		return timestampToDateString(Long.parseLong(timestamp));
	}

	public static String timestampToDateString(long timestamp) {
		Date date = new Date(timestamp);
		return sdf.format(date);
	}

	public static boolean charIsNumber(char tmpChar) {
		return tmpChar >= '0' && tmpChar <= '9';
	}

	public static int getNumberFromStringBuilder(StringBuilder sb, String prefix) throws NumberFormatException {
		int startIndex = sb.indexOf(prefix) + 1;
		if (startIndex != -1) {
			while(!BeaverUtils.charIsNumber(sb.charAt(startIndex)) && startIndex < sb.length()){
				startIndex ++;
			}

			if (startIndex < sb.length()) {
				int endIndex = startIndex;
				for (; endIndex < sb.length(); endIndex++) {
					if (!BeaverUtils.charIsNumber(sb.charAt(endIndex))) {
						return Integer.parseInt(sb.substring(startIndex, endIndex));
					}
				}
			}
		}

//		got an error, TODO: maybe should jump this day
		return -1;
	}

	@SuppressWarnings("unchecked")
	public static <T> T cloneTo(T src) throws IOException, ClassNotFoundException{
		ByteArrayOutputStream memoryBuffer = new ByteArrayOutputStream();
		ObjectOutputStream out = null;
		ObjectInputStream in = null;
		T dist = null;
		try{
			out = new ObjectOutputStream(memoryBuffer);
			out.writeObject(src);
			out.flush();
			in = new ObjectInputStream(new ByteArrayInputStream(memoryBuffer.toByteArray()));
			dist = (T) in.readObject();
			return dist;
		}finally{
			if (out != null) {
				try {
					out.close();
				} catch (IOException e) {
					PrintStackTrace(e);
				}
			}

			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					PrintStackTrace(e);
				}
			}
		}
	}

	public static byte[] encryptAes(byte[] bytes, String key) throws SecurityException{
		Key keySpec;
	    try {
	    	keySpec = buildAesKey(key);
	        Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
	        cipher.init(Cipher.ENCRYPT_MODE, keySpec);
	        return cipher.doFinal(bytes);
	    } catch (Exception e) {
	        logger.error(e.getMessage(), e);
	        throw new SecurityException(e.getMessage(), e);
	    }
	}

	public static byte[] decryptAes(byte[] bytes, String key) throws SecurityException{
	    Key keySpec;
	    try {
	        keySpec = buildAesKey(key);
	        Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
	        cipher.init(Cipher.DECRYPT_MODE, keySpec);
	        return cipher.doFinal(bytes);
	    } catch (Exception e) {
	        logger.error(e.getMessage(), e);
	        throw new SecurityException(e.getMessage(), e);
	    }
	}

	public static Key buildAesKey(String keyStr) throws UnsupportedEncodingException {
	    byte[] key = new byte[16];
	    byte[] temp = keyStr.getBytes("UTF-8");
	    if (key.length > temp.length) {
	        System.arraycopy(temp, 0, key, 0, temp.length);
	    } else {
	        System.arraycopy(temp, 0, key, 0, key.length);
	    }
	    SecretKeySpec keySpec = new SecretKeySpec(key, "AES");
	    return keySpec;
	}

	public static Object getTableIdFromUploadUrl(String tableUrl) {
//		example: upload://table/db5a8742-6460-11e6-bba9-09259609bdc7/0f2vxj_HBA2xzBdu
		return tableUrl.substring("upload://table/".length(), tableUrl.lastIndexOf('/'));
	}
}