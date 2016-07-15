package com.cloudbeaver.mockServer;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.junit.Assert;

import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.client.dbbean.DatabaseBean;
import com.cloudbeaver.client.dbbean.MultiDatabaseBean;
import com.cloudbeaver.client.dbbean.TableBean;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

@WebServlet("/")
public class PostDataServlet extends HttpServlet{
	private static Logger logger = Logger.getLogger(PostDataServlet.class);

	private static final String FLUME_HTTP_REQ_PREFIX = "[{ \"headers\" : {}, \"body\" : \"";
	public static final String DEFAULT_CHARSET = "utf-8";
	public static final String FILE_SAVE_DIR = "/home/beaver/Documents/test/result/";
	public static final boolean NEED_SAVE_FILE = false;
	private static final String JSON_FILED_HDFS_DB = "hdfs_db";
	private static int picNum = 0;

	public static Map<String, String> DBName2DBType = new HashMap<String, String>();
	{
		DBName2DBType.put("DocumentDB", "sqlserver");
		DBName2DBType.put("MeetingDB", "webservice");
		DBName2DBType.put("TalkDB", "webservice");
		DBName2DBType.put("PrasDB", "webservice");
		DBName2DBType.put("JfkhDB", "oracle");
		DBName2DBType.put("DocumentDBForSqlite", "sqlite");
		DBName2DBType.put("DocumentFiles", "file");
	}

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException{
//    	String url = req.getRequestURI();

    	BufferedReader br = new BufferedReader(new InputStreamReader(req.getInputStream()));
    	StringBuilder sb = new StringBuilder();
    	String tmp;
    	while ((tmp = br.readLine()) != null) {
			sb.append(tmp);
		}

    	String content = null;
    	if(sb.indexOf("headers") != -1 && sb.indexOf("body") != -1){
    		String base64code = sb.substring(sb.indexOf(FLUME_HTTP_REQ_PREFIX)+FLUME_HTTP_REQ_PREFIX.length(), sb.indexOf("\" }]"));
    		byte []bs = BeaverUtils.decompress(base64code.getBytes(DEFAULT_CHARSET));
    		content = new String(bs,DEFAULT_CHARSET);
    	} else {
    		content = sb.toString();
    	}
		System.out.println("content = " + content);

		String dbName = null;
		JSONArray newjArray = JSONArray.fromObject(content);
		if(newjArray.size()>0){
			JSONObject record = newjArray.getJSONObject(0);
			dbName = record.getString(JSON_FILED_HDFS_DB);
		}
		System.out.println("dbName = " + dbName);

		if(!content.contains("HeartBeat")){
			Assert.assertTrue("this database or file doesn't exists", DBName2DBType.containsKey(dbName));
		}

		if(!content.contains("HeartBeat") && DBName2DBType.containsKey(dbName)){
			try {
				updateTask(content, DBName2DBType.get(dbName));
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}

		if (!content.contains("HeartBeat") && DBName2DBType.containsKey(dbName) && DBName2DBType.get(dbName).equals("file")) {
			System.out.println("got one pic, Num:" + picNum++);
			if (NEED_SAVE_FILE) {
				saveFile(content, dbName);
			}
		}

//		if(content.substring(3).startsWith("hdfs_prison")){
//	    	updateTask(content);
//		}		

    	int debugLen = sb.length() > 300 ? 300 : sb.length();
    	System.out.println("get post data, data:" + sb.toString().substring(0, debugLen));
    }

    public static String changeDateToLongFormat(String str) throws ParseException{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");  
	    Date date = sdf.parse(str);
		long l = date.getTime();
		return String.valueOf(l);
    }

    public final static String getIpAddress(HttpServletRequest request) throws IOException { 
        String ip = request.getHeader("X-Forwarded-For");
        if (logger.isInfoEnabled()) {
            logger.info("getIpAddress(HttpServletRequest) - X-Forwarded-For - String ip=" + ip);
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
                ip = request.getHeader("Proxy-Client-IP");
                if (logger.isInfoEnabled()) {
                    logger.info("getIpAddress(HttpServletRequest) - Proxy-Client-IP - String ip=" + ip);
                }
            }
            if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
                ip = request.getHeader("WL-Proxy-Client-IP");
                if (logger.isInfoEnabled()) {
                    logger.info("getIpAddress(HttpServletRequest) - WL-Proxy-Client-IP - String ip=" + ip);
                }
            }
            if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
                ip = request.getHeader("HTTP_CLIENT_IP");
                if (logger.isInfoEnabled()) {
                    logger.info("getIpAddress(HttpServletRequest) - HTTP_CLIENT_IP - String ip=" + ip);
                }
            }
            if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
                ip = request.getHeader("HTTP_X_FORWARDED_FOR");
                if (logger.isInfoEnabled()) {
                    logger.info("getIpAddress(HttpServletRequest) - HTTP_X_FORWARDED_FOR - String ip=" + ip);
                }
            }
            if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
                ip = request.getRemoteAddr();
                if (logger.isInfoEnabled()) {
                    logger.info("getIpAddress(HttpServletRequest) - getRemoteAddr - String ip=" + ip);
                }
            }
        } else if (ip.length() > 15) {
            String[] ips = ip.split(",");
            for (int index = 0; index < ips.length; index++) {
                String strIp = (String) ips[index];
                if (!("unknown".equalsIgnoreCase(strIp))) {
                    ip = strIp;
                    break;
                }
            }
        }
        return ip;
    }

    public static void updateTask(String content, String serverType) throws IOException, ParseException{
    	if(GetTaskServlet.getTableId() == null){
    		return ;
    	}
    	MultiDatabaseBean databases = GetTaskServlet.getMultiDatabaseBean();
    	String maxxgsj = null;
    	Object table = null;
    	Object database = null;
    	JSONArray newjArray = JSONArray.fromObject(content);
		if(newjArray.size()>0){
			for(int i=0;i<newjArray.size();i++){
				JSONObject iob = newjArray.getJSONObject(i);
				String tableName = iob.getString("hdfs_table");
//				maxxgsj = Long.parseLong(iob.get("xgsj2").toString());
				if(serverType.equals("sqlserver")){
					maxxgsj = iob.get("xgsj").toString();
				}
				else if(serverType.equals("webservice")){
					String str = null;
					if(tableName.equals("pias/getItlist")){
						maxxgsj = iob.getString("starttime").toString();
					}
					else if(tableName.equals("qqdh/getTalkList")){
						str = iob.getString("startdate").toString();
						maxxgsj = changeDateToLongFormat(str);
					}
					else if(tableName.equals("qqdh/getQqdh")){
						str = iob.getString("modifydate").toString();
						if(str.contains("-")){
							maxxgsj = changeDateToLongFormat(str);
						}
						else{
							maxxgsj = str;
						}
					}
					else if(tableName.equals("pras/getResult")){
						str = iob.getString("CHECKDAY").toString();
						maxxgsj = changeDateToLongFormat(str);
					}
					else if(tableName.equals("pras/getTable")){
						//do nothing now
						return;
					}
					
				}
				else if(serverType.equals("oracle")){
					maxxgsj = iob.get("ID").toString();
				}
				else if(serverType.equals("sqlite")){
					maxxgsj = iob.get("xgsj2").toString();
				}
				else if(serverType.equals("file")){
					maxxgsj = iob.get("xgsj").toString();
				}
				table = iob.get("hdfs_table");
				database = iob.get("hdfs_db");
				
				searchOriginTask(databases,database,table,maxxgsj,serverType);
				
				//GetTaskServlet.setDodumentDBInitJson(databases);
//				System.out.println(GetTaskServlet.getDodumentDBInitJson());
//				logger.info("new task："+GetTaskServlet.getDodumentDBInitJson());
			}
		}
    }

    public static void searchOriginTask(MultiDatabaseBean databases,Object database,Object table,String maxxgsj,String serverType) throws IOException{
		for(int i = 0; i < databases.getDatabases().size(); i++){
			DatabaseBean dbBean = databases.getDatabases().get(i);
			if(dbBean.getDb().equals(database)){
				for (TableBean tBean : dbBean.getTables()) {
					if(tBean.getTable().equals(table)){
//						if(maxxgsj > Long.parseLong(tBean.getXgsj().substring("0x".length()))){
						if(serverType.equals("sqlserver")){
							Assert.assertTrue("table：" + tBean.getTable() + "xgsj of a record is less than table's xgsj", Long.parseLong(maxxgsj,16) > Long.parseLong(tBean.getXgsj(),16));
							if(Long.parseLong(maxxgsj,16) > Long.parseLong(tBean.getXgsj(),16)){
								tBean.setXgsj(maxxgsj);
								Assert.assertEquals(tBean.getXgsj(), maxxgsj);
								System.out.println("update table："+tBean.getTable()+" "+tBean.getXgsj());
							}
						}
						else if(serverType.equals("webservice")){
//							Assert.assertTrue("starttime of a record is less than table's starttime", Long.parseLong(maxxgsj) > Long.parseLong(tBean.getStarttime()));
							if(Long.parseLong(maxxgsj) > Long.parseLong(tBean.getStarttime())){
								tBean.setStarttime(maxxgsj);
								Assert.assertEquals(tBean.getStarttime(), maxxgsj);
								System.out.println("update table："+tBean.getTable()+" "+tBean.getStarttime());
							}		
						}
						else if(serverType.equals("oracle")){
							Assert.assertTrue("ID of a record is less than table's ID", Long.parseLong(maxxgsj) > Long.parseLong(tBean.getID()));
							if(Long.parseLong(maxxgsj) > Long.parseLong(tBean.getID())){
								tBean.setID(maxxgsj);
								Assert.assertEquals(tBean.getID(), maxxgsj);
								System.out.println("update table："+tBean.getTable()+" "+tBean.getID());
							}		
						}
						else if(serverType.equals("sqlite")){
							Assert.assertTrue("xgsj of a record is less than table's xgsj", Long.parseLong(maxxgsj) > Long.parseLong(tBean.getXgsj()));
							if(Long.parseLong(maxxgsj) > Long.parseLong(tBean.getXgsj())){
								tBean.setXgsj(maxxgsj);
								Assert.assertEquals(tBean.getXgsj(), maxxgsj);
								System.out.println("update table："+tBean.getTable()+" "+tBean.getXgsj());
							}		
						}
						else if(serverType.equals("file")){
							Assert.assertTrue("xgsj of a record is less than table's xgsj", Long.parseLong(maxxgsj,16) >= Long.parseLong(tBean.getXgsj(),16));
							if(Long.parseLong(maxxgsj,16) > Long.parseLong(tBean.getXgsj(),16)){
								tBean.setXgsj(maxxgsj);
								Assert.assertEquals(tBean.getXgsj(), maxxgsj);
								System.out.println("update table："+tBean.getTable()+" "+tBean.getXgsj());
							}		
						}
						return;
					}
				}
			}
		}
    }

    public static void saveFile(String content, String serverType) throws IOException{
    	JSONArray newjArray = JSONArray.fromObject(content);
		if(newjArray.size()>0){
			for(int i=0;i<newjArray.size();i++){
				JSONObject iob = newjArray.getJSONObject(i);
				String fileName = iob.getString("file_name");
//				String dirName = iob.getString("hdfs_table");
				String fileData = iob.getString("file_data");
				Object database = iob.get("hdfs_db");
				if(!DBName2DBType.get(database).equals("file")){
					continue;
				}
				BufferedWriter out = new BufferedWriter(new FileWriter(FILE_SAVE_DIR+fileName));
				out.write(fileData);
				out.flush();
				out.close();
			}
		}
    }

    @Override
    protected void doDelete(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	super.doDelete(req, resp);
    }

    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	super.doPut(req, resp);
    }

    public static void main(String []args) throws ParseException {
		System.out.println(changeDateToLongFormat("2016-07-08 10:24:27"));
	}
}
