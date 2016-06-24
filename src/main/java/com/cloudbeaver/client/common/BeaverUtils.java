package com.cloudbeaver.client.common;

import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import javax.imageio.ImageIO;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

//import com.sun.image.codec.jpeg.JPEGCodec;
//import com.sun.image.codec.jpeg.JPEGImageEncoder;

public class BeaverUtils {
	private static Logger logger = Logger.getLogger(BeaverUtils.class);

	public static String DEFAULT_CHARSET = "utf-8";

	public static boolean DEBUG_MODE = true;

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

	public static String doPost(String urlString, String flumeJson) throws IOException {
		return doPost(urlString, flumeJson, "application/json");
	}
	
	public static String doPost(String urlString, String flumeJson, String contentType) throws IOException {
		BufferedReader br = null;
		HttpURLConnection urlConnection = null;
		try {
			if (urlString.indexOf("http://") == -1) {
				urlString = "http://" + urlString;
			}

	        URL url = new URL(urlString);

	        urlConnection = (HttpURLConnection) url.openConnection();
	        urlConnection.setRequestMethod("POST");
	        urlConnection.setRequestProperty("Content-Type", contentType + ";charset=utf-8");//text/plain
	        urlConnection.setRequestProperty("Content-Length", "" + flumeJson.length());
	        urlConnection.setConnectTimeout(20000);

//	        logger.debug(urlConnection.getRequestProperty("Content-Type"));
	        urlConnection.setDoOutput(true);
	        PrintWriter pWriter = new PrintWriter((urlConnection.getOutputStream()));
	        pWriter.write(flumeJson);
	        pWriter.flush();
	        pWriter.close();

	        BufferedReader in = new BufferedReader(new InputStreamReader(urlConnection.getInputStream()));
	        String line = "";
	        StringBuffer sb = new StringBuffer();
	        while ((line = in.readLine()) != null) {
	            sb.append(line);
	        }

	        logger.debug("Got reply message from web, server:" + urlString + " responseCode:" + urlConnection.getResponseCode() + " reply:" + sb.toString());
	        return sb.toString();
		}finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					logger.error("close url reader error, msg:" + e.getMessage() + " url:" + urlString);
				}
			}
		}
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
		return fileName.endsWith("jpg") || fileName.endsWith("jpeg") || fileName.endsWith("bmp");
	}

	public static void printLogExceptionAndSleep(Exception e, String msgPrefix, int sleepTime) {
		BeaverUtils.PrintStackTrace(e);
		logger.error(msgPrefix + e.getMessage());
		BeaverUtils.sleep(3 * 1000);
	}
}
