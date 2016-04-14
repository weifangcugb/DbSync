package com.cloudbeaver.dbsync.fileLoader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import com.cloudbeaver.dbsync.common.BeaverUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class PicLoader implements Runnable {
	private static Logger logger = Logger.getLogger(PicLoader.class);

	private static final String PIC_DIRECTORY_NAME = "pic_directory";
	private static final String FLUME_SERVER_URL = "flume-server.url";
	private static final String CLIENT_ID = "clientid";

	private static String picDir = "c:/罪犯媒体/像片/";
	private static String flumeServer = "192.168.1.109:9092";
	private static String clientId = "1";

	private long miniLastChange = 0;
	private String threadName = null;
	private String confName = "dbname.conf";

	public static String getFlumeServer() {
		return flumeServer;
	}

	public static void setFlumeServer(String flumeServer) {
		PicLoader.flumeServer = flumeServer;
	}

	public static String getClientId() {
		return clientId;
	}

	public static void setClientId(String clientId) {
		PicLoader.clientId = clientId;
	}

	public long getMiniLastChange() {
		return miniLastChange;
	}

	public void setMiniLastChange(long miniLastChange) {
		this.miniLastChange = miniLastChange;
	}

	public PicLoader(String threadName) {
		this.threadName = threadName;
	}

    public static String getPicDir() {
		return picDir;
	}

	public static void setPicDir(String picPath) {
		File dir = new File(picPath);
		if (dir.exists() && dir.isDirectory()) {
			picDir = dir.getAbsolutePath();
		}else {
			logger.error("wrong dir name in config file, use default value. defaultPicPath:" + picDir + " confValue:" + dir.getAbsolutePath());
		}
	}

	private void loadPicConfig () {
    	Configurations configurations = new Configurations();
        try {
            Configuration configuration = configurations.properties(confName);
            Iterator<String> keys = configuration.getKeys();
            while (keys.hasNext()) {
            	String key = keys.next();
            	switch (key) {
				case PIC_DIRECTORY_NAME:
					setPicDir(configuration.getString(key));
					break;
				case FLUME_SERVER_URL :
					flumeServer = configuration.getString(key);
					if (flumeServer != null && !flumeServer.contains("://")) {
			        	flumeServer = "http://" + flumeServer;
			        }
					break;
				case CLIENT_ID:
					setClientId(configuration.getString(key));
					break;
				default:
					break;
				}
            }
        } catch (ConfigurationException e) {
            e.printStackTrace();
            logger.error("read config file failed, use default pic path, confName:" + confName + " picDir:" + picDir);
        }
    }

	@Override
	public void run() {
		loadPicConfig();
		getMiniChange();

		File pics = new File(picDir);
		if (!pics.exists() || !pics.isDirectory()) {
			logger.error("pic path is not exist or not a directory, thread exist. picDir:" + picDir);
			return;
		}

		byte[] picData = new byte[500000];

		while(true){
			logger.info(threadName + " started, picPath:" + picDir);

			final TreeMap<Long, File> tMap = new TreeMap<Long, File>();
			File[] picFiles = pics.listFiles(new FileFilter() {
				@Override
				public boolean accept(File pathname) {
					if (pathname.getName().indexOf("jpg") != -1) {
						long lastChange = pathname.lastModified();

						if (lastChange > 0 && lastChange > miniLastChange) {
							tMap.put(lastChange, pathname);
							return true;
						}else {
							logger.error("get file modify time error, file:" + pathname);
						}
					}

					return false;
				}
			});

			Set<Long> lastChange = tMap.keySet();
			Iterator<Long> iterator = lastChange.iterator();
			while (iterator.hasNext()) {
				Long changeTime = (Long) iterator.next();
				File file = tMap.get(changeTime);
				try {
					logger.info("start to upload pic, file:" + file.getAbsolutePath());
					String fileData = getFileData(picData, file);
					uploadFileData(file.getName(),fileData, file.lastModified());
					logger.info("finish upload pic, file:" + file.getAbsolutePath());
				} catch (IOException e) {
					logger.error("upload pic failed, file:" + file.getAbsolutePath());
					e.printStackTrace();
				}
			}

			break;
		}

		logger.info("end upload file");
	}

	private void getMiniChange() {
		Configurations configurations = new Configurations();
		Configuration conf;
		try {
			conf = configurations.properties(confName);
			String json = BeaverUtils.getSimplePage(conf.getString("tasks-server.url") + clientId);//HttpClientHelper.get(conf.getString("tasks-server.url") + clientId);
			ObjectMapper objectMapper = new ObjectMapper();
			JsonNode root = objectMapper.readTree(json);
			JsonNode dbs = root.get("databases");
			for (int i = 0; i < dbs.size(); i++) {
				JsonNode db = dbs.get(i);
				if (db.get("db").asText().equals("DocumentFiles")) {
					JsonNode tables = db.get("tables");
					for (int j = 0; j < tables.size(); j++) {
						JsonNode table = db.get(j);
						setPicDir(table.get("table").asText());
						setMiniLastChange(table.get("xgsj").asLong());
					}
				}else {
//					do nothing for now
				}
			}
		} catch (ConfigurationException | IOException e) {
			logger.error("read task list error, msg:" + e.getMessage());
			e.printStackTrace();
		}
	}

	private void uploadFileData(String fileName, String fileData, long lastModified) throws IOException{
		/*
		 * e.g.
		 * [[{\"hdfs_prison\":\"1\",\"hdfs_db\":\"DocumentDB\",\"hdfs_table\":\"da_jbxx\",\"id\":\"337178\"
		 */
		String uploadData = "[[{\"hdfs_prison\":\"" + clientId + "\",\"hdfs_db\":\"files\",\"hdfs_table\":\"pics\",\"pic_name\":\"" 
							+ fileName + "\", \"pic_data\":\"" + fileData + "\", \"xgsj\":\"" +  lastModified + "\"}]]";
		System.out.println();
		uploadData = uploadData.replaceAll("\"", "\\\\\"");
        String flumeJson = "[{ \"headers\" : {}, \"body\" : \"" + uploadData + "\" }]";
//		System.out.println(flumeJson);

        URL url = new URL(flumeServer);
        URLConnection connection = url.openConnection();
        connection.setDoOutput(true);
        PrintWriter pWriter = new PrintWriter((connection.getOutputStream()));
//            logger.debug("Send message to flume-server : " + flumeJson);
        pWriter.write(flumeJson);
        pWriter.close();
        BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        String line, result = "";
        while ((line = in.readLine()) != null) {
            result += line;
        }
        logger.debug("Got message from flume-server : " + result);
	}

	private String getFileData(byte[] picData, File file) throws IOException {
		BeaverUtils.clearByteArray(picData);
		FileInputStream fin = new FileInputStream(file);
		int length = fin.read(picData);
		if (length == 0) {
			logger.info("one empty pic file, file:" + file.getAbsolutePath());
			return "";
		}else {
			return new String(Base64.encodeBase64(ArrayUtils.subarray(picData, 0, length)));
		}
	}

	public static void main(String[] args) {
		new Thread(new PicLoader("PicLoader")).start();

		try {
			System.in.read();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
