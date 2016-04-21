package com.cloudbeaver.client.common;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;

import com.cloudbeaver.client.dbUploader.DbUploader;
import com.cloudbeaver.client.fileUploader.FileUploader;

public class BeaverUtils {
	private static Logger logger = Logger.getLogger(BeaverUtils.class);

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

	        br = new BufferedReader(new InputStreamReader(urlConnection.getInputStream()));
	        StringBuilder sb = new StringBuilder();
	        String tmp = "";
	        while((tmp = br.readLine()) != null){
	        	sb.append(tmp);
	        }
	        if (urlConnection.getResponseCode() == HttpStatus.SC_OK) {
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
		BufferedReader br = null;
		try {
			if (urlString.indexOf("http://") == -1) {
				urlString = "http://" + urlString;
			}

	        URL url = new URL(urlString);
	        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
	        urlConnection.setRequestMethod("POST");
	        urlConnection.setRequestProperty("Content-Type", "text/plain; charset=utf-8");
	        urlConnection.setConnectTimeout(20000);

	        urlConnection.setDoOutput(true);
	        PrintWriter pWriter = new PrintWriter((urlConnection.getOutputStream()));
	        pWriter.write(flumeJson);
	        pWriter.close();

	        BufferedReader in = new BufferedReader(new InputStreamReader(urlConnection.getInputStream()));
	        String line = "";
	        StringBuffer sb = new StringBuffer();
	        while ((line = in.readLine()) != null) {
	            sb.append(line);
	        }

	        logger.debug("Got reply message from web, server:" + urlString + " responseCode:" + urlConnection.getResponseCode() + " reply:" + sb.toString());

	        if (urlConnection.getResponseCode() == HttpStatus.SC_OK) {
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

	/*
	 * load a config file to a string=>string map
	 * will clear the map first
	 */
    public static Map<String, String> loadConfig(String confFileName) throws FileNotFoundException, IOException{
    	Map<String, String> conf = new HashMap<String, String>();

        Properties pps = new Properties();
        pps.load(DbUploader.class.getClassLoader().getResourceAsStream(confFileName));
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
}
