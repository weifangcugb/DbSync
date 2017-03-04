package com.cloudbeaver.mockServer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.junit.Assert;

import com.auth0.jwt.internal.org.apache.commons.codec.binary.Base64;
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
	public static final boolean NEED_SAVE_FILE = true;
	public static final String DATABASE_FILE_PREFIX = "/tmp/db/";
	public static final String DATABASE_NAME = "hdfs_db";
	public static final String TABLE_NAME = "hdfs_table";
	private static int picNum = 0;

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
		String tName = null;
		JSONArray newjArray = JSONArray.fromObject(content);

		Map<String, String> DBName2DBType = GetTaskServlet.map;
		if(!content.contains("HeartBeat")){
			if(newjArray.size()>0){
				JSONObject record = newjArray.getJSONObject(0);
				dbName = record.getString(DATABASE_NAME);
				tName = record.getString(TABLE_NAME);
			}
			System.out.println("dbName = " + dbName);
			Assert.assertTrue("this database or file doesn't exists", DBName2DBType.containsKey(dbName));
			//write data to local
	    	String fileName = DATABASE_FILE_PREFIX + dbName + "_" + tName;
	    	RandomAccessFile file = new RandomAccessFile(fileName, "rw");
			file.seek(file.length());
			file.write((content.substring(1, content.length()-1) + ",").getBytes());
			file.close();
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

    public static void updateTask(String content, String serverType) throws IOException, ParseException{
    	if(GetTaskServlet.getTableId() == null){
    		return ;
    	}
    	MultiDatabaseBean databases = GetTaskServlet.getMultiDatabaseBean();
    	JSONArray newjArray = JSONArray.fromObject(content);
		for(int i = 0; i < newjArray.size() ;i++){
			String maxxgsj = "0";
			JSONObject iob = newjArray.getJSONObject(i);
			String database = iob.getString("hdfs_db");
			String tableName = iob.getString("hdfs_table");

			if(serverType.equals("sqlserver")){
				if(database.equals("VideoMeetingDB") || database.equals("HelpDB") || database.equals("SqlServerTest")){
					maxxgsj = iob.getString("ID");
				} else {
					maxxgsj = iob.getString("xgsj");
				}
			} else if(serverType.equals("webservice")){
				String str = null;
				if(tableName.equals("pias/getItlist")){
					maxxgsj = iob.getString("starttime").toString();
				} else if(tableName.equals("qqdh/getTalkList")){
					str = iob.getString("startdate").toString();
					maxxgsj = changeDateToLongFormat(str);
				} else if(tableName.equals("qqdh/getQqdh")){
					str = iob.getString("modifydate").toString();
					if(str.contains("-")){
						maxxgsj = changeDateToLongFormat(str);
					}
					else{
						maxxgsj = str;
					}
				} else if(tableName.equals("pras/getResult")){
					str = iob.getString("CHECKDAY").toString();
					maxxgsj = changeDateToLongFormat(str);
				} else if(tableName.equals("pras/getTable")){
					//do nothing now
					return;
				}
			} else if(serverType.equals("oracle")){
				if (tableName.equals("TBXF_SCREENING") || tableName.equals("TBXF_PRISONERPERFORMANCE") || tableName.equals("TBXF_SENTENCEALTERATION")) {
					maxxgsj = iob.getString("OPTIME");
				}else if (tableName.equals("TBPRISONER_MEETING_SUMMARY")) {
					maxxgsj = iob.getString("MDATE");
				}else if (tableName.equals("TBFLOW")) {
					maxxgsj = iob.getString("FLOWSN");
				}else{
					maxxgsj = iob.getString("ID");
				}
			} else if(serverType.equals("sqlite")){
				maxxgsj = iob.get("xgsj2").toString();
			} else if(serverType.equals("file")){
				maxxgsj = iob.get("xgsj").toString();
			}

			searchOriginTask(databases, database, tableName, maxxgsj, serverType);

			//GetTaskServlet.setDodumentDBInitJson(databases);
//				System.out.println(GetTaskServlet.getDodumentDBInitJson());
//				logger.info("new task："+GetTaskServlet.getDodumentDBInitJson());
		}
    }

    private static String getOracleDateLong(String datetime) throws ParseException {
//		2016-07-15 16:55:01.0
//		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
//    	Date date = sdf.parse(datetime);
//    	return "" + date.getTime();
    	return datetime.substring(0, datetime.indexOf('.')).replaceAll("[-: ]", "");
	}

	public static void searchOriginTask(MultiDatabaseBean databases,Object database,Object table,String maxxgsj,String serverType) throws IOException, NumberFormatException, ParseException{
		for(int i = 0; i < databases.getDatabases().size(); i++){
			DatabaseBean dbBean = databases.getDatabases().get(i);
			if(dbBean.getDb().equals(database)){
				for (TableBean tBean : dbBean.getTables()) {
					if(tBean.getTable().equals(table)){
//						if(maxxgsj > Long.parseLong(tBean.getXgsj().substring("0x".length()))){
						if(serverType.equals("sqlserver")){
							Assert.assertTrue("table：" + tBean.getTable() + "xgsj of a record is less than table's xgsj", Long.parseLong(maxxgsj,16) >= Long.parseLong(tBean.getXgsj(),16));
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
							String oldxgsj = "";
							if (tBean.getTable().equals("TBXF_SCREENING") || tBean.getTable().equals("TBXF_PRISONERPERFORMANCE") || tBean.getTable().equals("TBXF_SENTENCEALTERATION")) {
								oldxgsj = tBean.getOpTime();
							}else if (tBean.getTable().equals("TBPRISONER_MEETING_SUMMARY")) {
								oldxgsj = tBean.getmDate();
							}else if (tBean.getTable().equals("TBFLOW")) {
								oldxgsj = tBean.getFlowSn();
							}else {
								oldxgsj = tBean.getID();
							}
							if(dbBean.getDb().startsWith("XfzxDB")){
								Assert.assertTrue("ID of a record is less than table's ID, newId:" + maxxgsj + " oldId:" + oldxgsj, maxxgsj.compareTo(oldxgsj) >= 0);
								if(maxxgsj.compareTo(oldxgsj) > 0){
									if (tBean.getTable().equals("TBXF_SCREENING") || tBean.getTable().equals("TBXF_PRISONERPERFORMANCE") || tBean.getTable().equals("TBXF_SENTENCEALTERATION")) {
										tBean.setOpTime(maxxgsj);
									}else if (tBean.getTable().equals("TBPRISONER_MEETING_SUMMARY")) {
										tBean.setmDate(maxxgsj);
									}else if (tBean.getTable().equals("TBFLOW")) {
										tBean.setFlowSn(maxxgsj);
									}
									System.out.println("update table："+tBean.getTable()+" "+maxxgsj);
								}
							} else if(dbBean.getDb().equals("JfkhDB")){
								Assert.assertTrue("ID of a record is less than table's ID, newId:" + maxxgsj + " oldId:" + oldxgsj, Long.parseLong(maxxgsj) >= Long.parseLong(oldxgsj));
								if(Long.parseLong(maxxgsj) > Long.parseLong(oldxgsj)){
									tBean.setID(maxxgsj);
								}
								System.out.println("update table："+tBean.getTable()+" "+maxxgsj);
							}
						}
						else if(serverType.equals("sqlite")){
							Assert.assertTrue("xgsj of a record is less than table's xgsj", Long.parseLong(maxxgsj) >= Long.parseLong(tBean.getXgsj()));
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
				Map<String, String> DBName2DBType = GetTaskServlet.map;
				if(!DBName2DBType.get(database).equals("file")){
					continue;
				}
				byte [] fdata = Base64.decodeBase64(fileData.getBytes());
				FileOutputStream out = new FileOutputStream(new File(FILE_SAVE_DIR+fileName));
				out.write(fdata);
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
