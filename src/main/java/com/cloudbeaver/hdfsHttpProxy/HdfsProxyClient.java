package com.cloudbeaver.hdfsHttpProxy.proxybean;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.net.HttpURLConnection;
import java.net.URL;
import org.apache.log4j.Logger;
import com.cloudbeaver.client.common.BeaverUtils;

import net.sf.json.JSONObject;

public class HdfsProxyClient {
	private static Logger logger = Logger.getLogger(HdfsProxyClient.class);
	private static String fileInfoUrl = "http://localhost/fileinfo";
	private static int READ_BUFFER_SIZE = 1024 * 10;
	private static String contentType = "application/octet-stream";

	public void doUploadFileData(String fileFullName, String urlString) {
		HttpURLConnection urlConnection = null;
		BufferedReader br = null;
		while(true) {
			try{
				if (urlString.indexOf("http://") == -1) {
					urlString = "http://" + urlString;
				}
		        URL url = new URL(urlString);
		        urlConnection = (HttpURLConnection) url.openConnection();
		        urlConnection.setRequestMethod("POST");
		        urlConnection.setRequestProperty("Content-Type", contentType + ";charset=utf-8");
		        urlConnection.setConnectTimeout(20000);
		        urlConnection.setDoInput(true);
		        urlConnection.setDoOutput(true);
//		        urlConnection.setUseCaches(false);
		        urlConnection.setChunkedStreamingMode(0);
		        urlConnection.connect();

		        logger.debug(urlConnection.getRequestProperty("Content-Type"));
		        DataOutputStream out = new DataOutputStream((urlConnection.getOutputStream()));

//				first, sync position with web server
		        long seekPos = 0;
				String fileName = fileFullName.substring(fileFullName.lastIndexOf("/")+1);
				String json = BeaverUtils.doGet(fileInfoUrl + "?fileName=" + fileName);
				JSONObject jsonObject = JSONObject.fromObject(json);
				if(fileName.equals(jsonObject.get("file")) && !jsonObject.get("length").equals(-1)){
					 seekPos = Long.valueOf(jsonObject.get("length").toString());
				}

//				then, open the url stream and write util the end of the file
				RandomAccessFile in = new RandomAccessFile(fileFullName,"r");
				in.seek(seekPos);

				logger.info("start to upload file, fileName:" + fileName + " pos:" + seekPos);
		        byte [] readBuf = new byte[READ_BUFFER_SIZE];
		        int len = 0;
		        while((len = in.read(readBuf)) != -1){
	 				out.write(readBuf, 0, len);
			        out.flush();
			        System.out.println("len = " + len);
			        BeaverUtils.sleep(100);
		    	}
		        out.close();

		        br = new BufferedReader(new InputStreamReader(urlConnection.getInputStream()));
		        StringBuilder sBuilder = new StringBuilder();
		        String tmp = null;
		        while((tmp = br.readLine()) != null){
		        	sBuilder.append(tmp);
		        }
		        logger.info("get data from server : " + sBuilder.toString());
		        in.close();
		        break;
			}catch(IOException e){
				BeaverUtils.printLogExceptionAndSleep(e, "upload file exception, ", 5000);
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
	}

	public static void main(String[] args) {
		String filename = "/home/beaver/Documents/test/hadoop/harry.txt";
		String url = "http://localhost:8811/uploaddata?fileName=" + filename.substring(filename.lastIndexOf("/") + 1);
		HdfsProxyClient hdfsHttpClient = new HdfsProxyClient();
		hdfsHttpClient.doUploadFileData(filename, url);
	}
}
