package com.cloudbeaver.server.consumer;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

import com.cloudbeaver.client.common.BeaverFatalException;
import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.client.common.FixedNumThreadPool;
import com.cloudbeaver.client.common.CommonUploader;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

/**
 * get db/file data from kafka, upload them to web server
 */
public class SyncConsumer extends FixedNumThreadPool{
	public static boolean USE_BEAVER_KAFKA = true;

	protected static Logger logger = Logger.getLogger(SyncConsumer.class);

	protected static final String CONF_UPLOAD_FILE_URL = "upload.file.url";
	protected static final String CONF_UPLOAD_DB_URL = "upload.db.url";
	protected static final String CONF_HEARTBEAT_URL = "upload.heartbeat.url";
	protected static final String CONF_ALLOWED_DBS = "allowed.dbs";
	protected static final String LOCAL_FILE_STORED_PATH = "/tmp/";
	protected static final String JSON_FILED_HDFS_DB = "hdfs_db";
	protected static final String JSON_FILED_HDFS_CLIENT = "hdfs_client";

//	kafka configs
	private static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
	private static final String ZOOKEEPER_OFFSET_RESET = "auto.offset.reset";
	private static final String KAFKA_GROUP_ID = "group.id";
	private static final String KAFKA_AUTO_COMMIT_INTERVALS = "auto.commit.interval.ms";
	private static final String DEFAULT_CONSUMER_GROUP_ID = "g2";
	private static final String KAFKA_OFFSET_SMALLEST = "smallest";
	private static final int DEFAULT_KAFKA_AUTO_COMMIT_INTERVALS = 1000;
	private static int TOPIC_PARTITION_NUM = 10;
	private static String TOPIC_NAME = "hdfs_upload";

	protected static final boolean STOR_IN_LOCAL = false;
	protected static final boolean UPLOAD_FILE_TO_WEB_SERVER = true;

	protected static final int MAX_POST_RETRY_TIME = 70;

	List<KafkaStream<byte[], byte[]>> streams = null;
	ConsumerConnector consumer = null;

	protected Map<String, String> conf = null;

	protected String fileUploadUrl= null;
	protected String dbUploadUrl = null;
	protected String heartBeatUrl = null;

	public volatile List<String> allowedDBs = new ArrayList<>();

	@Override
	protected void setup() throws BeaverFatalException {
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

				new Thread(() ->{
					while(isRunning()){
						try{
							Map<String, String> confMap = BeaverUtils.loadConfig(CommonUploader.CONF_KAFKA_CONSUMER_FILE_NAME);
							allowedDBs = Arrays.asList(conf.get(CONF_ALLOWED_DBS).split(","));
							logger.info("load new allowed dbs: " + allowedDBs.stream().collect(Collectors.joining(",")));
						}catch(IOException e) {
							BeaverUtils.printLogExceptionWithoutSleep(e, "load allowed dbs failed");
						}
						BeaverUtils.sleep(2 * 60 * 1000);
					}
				}).start();
			}else {
				throw new BeaverFatalException("FATAL: no conf " + CONF_ALLOWED_DBS + " confFile:" + CommonUploader.CONF_KAFKA_CONSUMER_FILE_NAME);
			}

			if (!conf.containsKey(ZOOKEEPER_CONNECT)) {
				throw new BeaverFatalException("FATAL: no conf " + ZOOKEEPER_CONNECT + " confFile:" + CommonUploader.CONF_KAFKA_CONSUMER_FILE_NAME);
			}
		} catch (IOException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.fatal("load config failed, please restart process. confName:" + CommonUploader.CONF_KAFKA_CONSUMER_FILE_NAME + " msg:" + e.getMessage());
			throw new BeaverFatalException("load config failed, please restart process. confName:" + CommonUploader.CONF_KAFKA_CONSUMER_FILE_NAME + " msg:" + e.getMessage(), e);
		}

		Properties props = new Properties();
		props.put(ZOOKEEPER_CONNECT, conf.get(ZOOKEEPER_CONNECT));
		props.put(ZOOKEEPER_OFFSET_RESET, KAFKA_OFFSET_SMALLEST); 
		props.put(KAFKA_GROUP_ID, conf.get(KAFKA_GROUP_ID) == null? DEFAULT_CONSUMER_GROUP_ID: conf.get(KAFKA_GROUP_ID));
		props.put(KAFKA_AUTO_COMMIT_INTERVALS, conf.get(KAFKA_AUTO_COMMIT_INTERVALS) == null ? DEFAULT_KAFKA_AUTO_COMMIT_INTERVALS : conf.get(KAFKA_AUTO_COMMIT_INTERVALS));

		ConsumerConfig consumerConfig = new ConsumerConfig(props);
		consumer = (ConsumerConnector) Consumer.createJavaConsumerConnector(consumerConfig);

		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(TOPIC_NAME, TOPIC_PARTITION_NUM);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

		streams = consumerMap.get(TOPIC_NAME);
	}

	@Override
	protected void doTask(Object taskObject) {
		KafkaStream<byte[], byte[]> stream = (KafkaStream<byte[], byte[]>) taskObject;
		ConsumerIterator<byte[], byte[]> iter = stream.iterator();
		while ( iter.hasNext() ) {
			try {
    			MessageAndMetadata<byte[] , byte[]> mam = iter.next();
    			byte[] key = mam.key();
    			byte[] msg = mam.message();

    			int keyStartIndex = 0;
    			if (USE_BEAVER_KAFKA) {
					int tokenLen = msg[0];
					keyStartIndex += tokenLen + 1;
				}

    			int keyLen = ByteBuffer.wrap(msg, keyStartIndex, 4).getInt();
    			String msgKey = new String(msg, keyStartIndex + 4, keyLen);
    			String msgBody = "";
    			try {
    				msgBody = new String(BeaverUtils.decompress(Arrays.copyOfRange(msg, keyStartIndex + 4 +keyLen, msg.length)), BeaverUtils.DEFAULT_CHARSET);//ArrayUtils.subarray(msg, 4 + keyLen, msg.length)
    				logger.info("msgKey:" + msgKey + " msgLen:" + msg.length + " msgBody:" + msgBody.substring(0, 150));
    			} catch (IOException e) {
    				BeaverUtils.PrintStackTrace(e);
    				logger.error("unzip error, this is an invalid message. msg:" + e.getMessage());
    				continue;
    			}

    			ObjectMapper oMapper = new ObjectMapper();
    			JsonNode root= oMapper.readTree(msgBody);
				if (root.isArray() && root.get(0) != null && (root.get(0).has(JSON_FILED_HDFS_DB))) {
					if (root.get(0).has(CommonUploader.REPORT_TYPE) && root.get(0).get(CommonUploader.REPORT_TYPE).asText().equals(CommonUploader.REPORT_TYPE_HEARTBEAT)) {
						logger.debug("heart beat data, msg:" + msgBody);
						try {
							BeaverUtils.doPost(heartBeatUrl + "/" + root.get(0).get(JSON_FILED_HDFS_CLIENT).asText(), msgBody);
						} catch (IOException e) {
							BeaverUtils.PrintStackTrace(e);
							logger.error("send heart beat to web server error, msg:" + e.getMessage());
						}

//						heartbeat message can be lost, so do next message
						continue;
					}

					String dbName = root.get(0).get(JSON_FILED_HDFS_DB).asText();
					int tryTime = 0;
					for (; tryTime < MAX_POST_RETRY_TIME; tryTime++) {
						try {
							if (allowedDBs.contains(dbName)) {
								if (!dbName.equals(CommonUploader.TASK_FILEDB_NAME)) {
//									upload db data to web server
									BeaverUtils.doPost(dbUploadUrl, msgBody);
								} else {
									if (UPLOAD_FILE_TO_WEB_SERVER) {
										logger.debug("posturl:" + fileUploadUrl);
										BeaverUtils.doPost(fileUploadUrl, msgBody);
									}

									if (STOR_IN_LOCAL) {
										for (int i = 0; i < root.size(); i++) {
											JsonNode item = root.get(i);
											if (item.get(JSON_FILED_HDFS_DB).asText().equals(CommonUploader.TASK_FILEDB_NAME)) {
												System.out.println(item.get("file_name").asText());
												writeToFile(Base64.decodeBase64(item.get("file_data").asText()));
											}
										}
									}
								}
							}else {
								logger.error("unknow db type," + " dbName:" + dbName + " msg:" + msgBody + " allowedDbs: " + allowedDBs.stream().collect(Collectors.joining(",")));
							}

							break;
						}catch(IOException e){
							BeaverUtils.PrintStackTrace(e);
							logger.error("invalid json message, errMsg:" + e.getMessage() + " key:" + msgKey + " msg:" + msgBody);
							if (e instanceof ConnectException || BeaverUtils.isHttpServerInternalError(e.getMessage())) {
//								can't connect to server, or server return internal error, retry again
								BeaverUtils.sleep(2000);
							}else {
//								other response code, don't retry
								break;
							}
						}
					}

					if (tryTime == MAX_POST_RETRY_TIME) {
						logger.error("post data error, retry too many times, drop it. msg:" + msgBody);
					}
				}else{
					logger.error("invalid message, no filed " + JSON_FILED_HDFS_DB + " in message, msg:" + msgBody);
					continue;
				}
			} catch (Exception e) {
				BeaverUtils.PrintStackTrace(e);
//				logger.error("invalid json message, errMsg:" + e.getMessage() + " key:" + msgKey + " msg:" + msgBody);
				logger.error("invalid json message, errMsg:" + e.getMessage() );
			}
		}
	}

	public static void writeToFile(byte[] msgBody) {
		try {
			String fileName = LOCAL_FILE_STORED_PATH + System.currentTimeMillis() + "_" + Thread.currentThread().getId() +".jpg";
			FileOutputStream fout = new FileOutputStream(fileName);
			fout.write(msgBody);
			fout.flush();
			fout.close();
			System.out.println("stored file now, fileName:" + fileName);
			logger.info("stored file, fileName:" + fileName);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void shutdown() {
		consumer.shutdown();
	}

	@Override
	protected int getThreadNum() {
		return TOPIC_PARTITION_NUM;
	}

	@Override
	protected Object getTaskObject(int index) {
		return streams.get(index);
	}

	@Override
	protected long getSleepTimeBetweenTaskInnerLoop() {
		return 1 * 1000;
	}

	@Override
	protected String getTaskDescription() {
		return "db/file consumer";
	}

	public static void startSyncConsumer(){
		logger.info("starting syncConsumer");

    	Thread syncConsumer = new Thread(new SyncConsumer());
    	syncConsumer.start();

    	try {
			syncConsumer.join();
		} catch (InterruptedException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("dbuploader join failed, msg:" + e.getMessage());
		}
	}

	@Override
	protected void doHeartBeat() {
//		TODO: send heart beat to server
	}

    public static void main( String[] args ) {
    	startSyncConsumer();
    }
}
