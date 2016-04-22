package com.cloudbeaver.server.consumer;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Logger;

import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.client.common.FixedNumThreadPool;
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
	private static Logger logger = Logger.getLogger(SyncConsumer.class);

	private static final int TOPIC_PARTITION_NUM = 10;
	private static final String TOPIC_NAME = "hdfs_upload";

//	this config just for test
	private static boolean STOR_IN_LOCAL = true;

	List<KafkaStream<byte[], byte[]>> streams = null;
	ConsumerConnector consumer = null;

	@Override
	protected void setup() {
		Properties props = new Properties();
		props.put("zookeeper.connect", "br1:2181/kafka");
		props.put("group.id", "g1");
		props.put("auto.commit.interval.ms", "1000");
		 
		ConsumerConfig consumerConfig = new ConsumerConfig(props);
		consumer = (ConsumerConnector) Consumer.createJavaConsumerConnector(consumerConfig);

		String topic = "hdfs_upload";

		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(TOPIC_NAME, TOPIC_PARTITION_NUM);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		 
		streams = consumerMap.get(topic);
	}

	@Override
	protected void doTask(Object taskObject) {
		KafkaStream<byte[], byte[]> stream = (KafkaStream<byte[], byte[]>) taskObject;
		ConsumerIterator<byte[], byte[]> iter = stream.iterator();
		while ( iter.hasNext() ) {
			MessageAndMetadata<byte[] , byte[]> mam = iter.next();
			byte[] key = mam.key();
			byte[] msg = mam.message();

			int keyLen = ByteBuffer.wrap(msg, 0, 4).getInt();
			String msgKey = new String(msg, 4, keyLen);
			String msgBody = "";
			try {
				msgBody = new String(BeaverUtils.decompress(ArrayUtils.subarray(msg, 4 + keyLen, msg.length)), BeaverUtils.DEFAULT_CHARSET);
				logger.info("msgKey:" + msgKey + " msgLen:" + msg.length + " msgBody:");
			} catch (IOException e) {
				BeaverUtils.PrintStackTrace(e);
				logger.error("unzip error, this is an invalid message. msg:" + e.getMessage());
				continue;
			}

			if (STOR_IN_LOCAL) {
				ObjectMapper oMapper = new ObjectMapper();
				JsonNode root;
				try {
					root = oMapper.readTree(msgBody);
					for (int i = 0; i < root.size(); i++) {
						JsonNode item = root.get(i);
						if (item.get("hdfs_db").asText().equals("DocumentFiles")) {
							System.out.println(item.get("file_name").asText());
							writeToFile(Base64.decodeBase64(item.get("file_data").asText()));
						}
					}
				} catch (Exception e) {
					BeaverUtils.PrintStackTrace(e);
					logger.error("write file data to local fs error, msg:" + e.getMessage());
				}
			}
		}
	}

	private void writeToFile(byte[] msgBody) {
		try {
			String fileName = "/tmp/" + System.currentTimeMillis() + "_" + Thread.currentThread().getId() +".jpg";
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

    public static void main( String[] args ) {
    	startSyncConsumer();
    }

	public static void startSyncConsumer(){
    	Thread syncConsumer = new Thread(new SyncConsumer());
    	syncConsumer.start();

    	try {
			syncConsumer.join();
		} catch (InterruptedException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.error("dbuploader join failed, msg:" + e.getMessage());
		}
	}
}