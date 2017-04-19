package com.cloudbeaver.repair;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.codec.binary.Base64;

import com.cloudbeaver.client.common.BeaverFatalException;
import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.client.common.CommonUploader;
import com.cloudbeaver.server.consumer.SyncConsumer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

class ErrorMsgMatcher{
	String regex;
	Pattern pattern;
	boolean msgCompressed;

	public ErrorMsgMatcher(String regex, boolean msgcompressed) {
		this.regex = regex;
		this.msgCompressed = msgcompressed;
		pattern = Pattern.compile(regex, Pattern.DOTALL);
	}

	public String getMsgBody(String msg) throws IOException{
		Matcher matcher = pattern.matcher(msg);
		if (matcher.find()) {
			if (msgCompressed) {
				return new String(BeaverUtils.decompress(matcher.group(1).getBytes(BeaverUtils.DEFAULT_CHARSET)), BeaverUtils.DEFAULT_CHARSET);
			}else{
				return new String(matcher.group(1).getBytes(BeaverUtils.DEFAULT_CHARSET), BeaverUtils.DEFAULT_CHARSET);
			}
			
		}else{
			return null;
		}
	}
}

public class RepairDBSync extends SyncConsumer{
	private int repairNum = 0;

	public static void main(String[] args) {
		startRepair();
	}

	public static void startRepair() {
		List<ErrorMsgMatcher> matchers = new ArrayList<>();
		matchers.add(new ErrorMsgMatcher("msg is too long, msg:\\[\\{ \"headers\" : \\{\\}, \"body\" : \"(.*)\" \\}\\]", true));
		matchers.add(new ErrorMsgMatcher("get db data,.*json:(.*)", false));
		matchers.add(new ErrorMsgMatcher("post data error, retry too many times, drop it\\. msg:(.*)", false));
		matchers.add(new ErrorMsgMatcher("invalid json message, .* msg:(.*)", false));
		matchers.add(new ErrorMsgMatcher("unknow db type, dbName:.* msg:(.*)", false));
		matchers.add(new ErrorMsgMatcher("invalid json message, errMsg:Connection refused key:.* msg:(.*)", false));

		RepairDBSync dbSync = new RepairDBSync();
		dbSync.repair(matchers);
	}

	private void repair(List<ErrorMsgMatcher> matchers) {
		try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in, BeaverUtils.DEFAULT_CHARSET))) {
			loadConfig();

			String msg = null;
			while((msg = br.readLine()) != null){
				if (msg.trim().equals("")) {
					continue;
				}

				boolean repaired = false;
				try{
					for (ErrorMsgMatcher matcher : matchers) {
						String msgBody = matcher.getMsgBody(msg);
						if (msgBody != null) {
							repairMsg(msgBody);
							repaired = true;
							break;
						}
					}

					if (!repaired) {
						logger.error("can't recognize msg, msg:" + msg);
					}
				}catch(JsonProcessingException e){
					logger.error("parse json exception, msg:" + e.getMessage());
				}
			}

			logger.info("repair down, repairNum:" + repairNum);
		} catch (IOException | BeaverFatalException e) {
			BeaverUtils.printLogExceptionWithoutSleep(e, "repair failed");
		}
	}

	private void loadConfig() throws BeaverFatalException {
		try {
			conf = BeaverUtils.loadConfig(CommonUploader.CONF_KAFKA_CONSUMER_FILE_NAME);
			if (conf.containsKey(CONF_UPLOAD_DB_URL)) {
				dbUploadUrl = conf.get(CONF_UPLOAD_DB_URL);
			}else {
				throw new BeaverFatalException("FATAL: no conf " + CONF_UPLOAD_DB_URL + " confFile:" + CommonUploader.CONF_KAFKA_CONSUMER_FILE_NAME);
			}

			if (conf.containsKey(CONF_UPLOAD_FILE_URL)) {
				fileUploadUrl = conf.get(CONF_UPLOAD_FILE_URL);
			}else {
				throw new BeaverFatalException("FATAL: no conf " + CONF_UPLOAD_FILE_URL + " confFile:" + CommonUploader.CONF_KAFKA_CONSUMER_FILE_NAME);
			}

			if (conf.containsKey(CONF_HEARTBEAT_URL)) {
				heartBeatUrl = conf.get(CONF_HEARTBEAT_URL);
			}else {
				throw new BeaverFatalException("FATAL: no conf " + CONF_HEARTBEAT_URL + " confFile:" + CommonUploader.CONF_KAFKA_CONSUMER_FILE_NAME);
			}

			if (conf.containsKey(CONF_ALLOWED_DBS)) {
				String[] dbs = conf.get(CONF_ALLOWED_DBS).split(",");
				for (String db : dbs) {
					allowedDBs.add(db);
				}
			}else {
				throw new BeaverFatalException("FATAL: no conf " + CONF_ALLOWED_DBS + " confFile:" + CommonUploader.CONF_KAFKA_CONSUMER_FILE_NAME);
			}
		} catch (IOException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.fatal("load config failed, please restart process. confName:" + CommonUploader.CONF_KAFKA_CONSUMER_FILE_NAME + " msg:" + e.getMessage());
			throw new BeaverFatalException("load config failed, please restart process. confName:" + CommonUploader.CONF_KAFKA_CONSUMER_FILE_NAME + " msg:" + e.getMessage(), e);
		}
	}

	private void repairMsg(String msgBody) throws JsonProcessingException, IOException {
		ObjectMapper oMapper = new ObjectMapper();
		JsonNode root= oMapper.readTree(msgBody);
		msgBody = msgBody.replaceAll("\\\n", "").replaceAll("\\\r", "");
		if (root.isArray() && root.get(0) != null && (root.get(0).has(SyncConsumer.JSON_FILED_HDFS_DB))) {
			if (root.get(0).has(CommonUploader.REPORT_TYPE) && root.get(0).get(CommonUploader.REPORT_TYPE).asText().equals(CommonUploader.REPORT_TYPE_HEARTBEAT)) {
				logger.debug("heart beat data, msg:" + msgBody);
				try {
					BeaverUtils.doPost(heartBeatUrl + "/" + root.get(0).get(SyncConsumer.JSON_FILED_HDFS_CLIENT).asText(), msgBody);
				} catch (IOException e) {
					BeaverUtils.PrintStackTrace(e);
					logger.error("send heart beat to web server error, msg:" + e.getMessage());
				}

//				heartbeat message can be lost, so do next message
				return;
			}

			String dbName = root.get(0).get(SyncConsumer.JSON_FILED_HDFS_DB).asText();
			int tryTime = 0;
			for (; tryTime < SyncConsumer.MAX_POST_RETRY_TIME; tryTime++) {
				try {
					if (allowedDBs.contains(dbName)) {
						if (!dbName.equals(CommonUploader.TASK_FILEDB_NAME)) {
//							upload db data to web server
							BeaverUtils.doPost(dbUploadUrl, msgBody);
						} else {
							if (SyncConsumer.UPLOAD_FILE_TO_WEB_SERVER) {
								logger.debug("posturl:" + fileUploadUrl);
								BeaverUtils.doPost(fileUploadUrl, msgBody);
							}

							if (SyncConsumer.STOR_IN_LOCAL) {
								for (int i = 0; i < root.size(); i++) {
									JsonNode item = root.get(i);
									if (item.get(SyncConsumer.JSON_FILED_HDFS_DB).asText().equals(CommonUploader.TASK_FILEDB_NAME)) {
										logger.info(item.get("file_name").asText());
										SyncConsumer.writeToFile(Base64.decodeBase64(item.get("file_data").asText()));
									}
								}
							}
						}
						logger.info("repair one msg, Num:" + ++repairNum);
//						logger.debug("repair one msg, msgBody:" + msgBody);
					}else {
						logger.error("repaired failed, unknow db type," + " dbName:" + dbName + " msg:" + msgBody);
					}

					break;
				}catch(IOException e){
					BeaverUtils.PrintStackTrace(e);
					logger.error("invalid json message, errMsg:" + e.getMessage() + " msg:" + msgBody);
					if (e instanceof ConnectException || BeaverUtils.isHttpServerInternalError(e.getMessage())) {
//						can't connect to server, or server return internal error, retry again
						BeaverUtils.sleep(300);
					}else {
//						other response code, don't retry
						break;
					}
				}
			}

			if (tryTime == SyncConsumer.MAX_POST_RETRY_TIME - 1) {
				logger.error("repair msg failed because of too many retries. msg:" + msgBody);
			}
		}else{
			logger.error("invalid message, no filed " + SyncConsumer.JSON_FILED_HDFS_DB + " in message, msg:" + msgBody);
		}		
	}
}
