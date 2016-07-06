package com.cloudbeaver.mockServer;

import java.io.BufferedReader;
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

import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.client.dbbean.DatabaseBean;
import com.cloudbeaver.client.dbbean.MultiDatabaseBean;
import com.cloudbeaver.client.dbbean.TableBean;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import scala.xml.dtd.PublicID;

@WebServlet("/")
public class PostDataServlet extends HttpServlet{
	private static Logger logger = Logger.getLogger(PostDataServlet.class);
	private static String getTaskApi = "/";
	public static String DEFAULT_CHARSET = "utf-8";
	public static Map<String, String> map = new HashMap<String, String>();

	{
		map.put("DocumentDB", "sqlserver");
		map.put("MeetingDB", "webservice");
		map.put("TalkDB", "webservice");
		map.put("PrasDB", "webservice");
		map.put("JfkhDB", "oracle");
		map.put("DocumentDBForSqlite", "sqlite");
	}
	
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException{
    	String url = req.getRequestURI();
    	if (!url.equals(getTaskApi)) {
			throw new ServletException("invalid url, format: " + getTaskApi);
		}

    	BufferedReader br = new BufferedReader(new InputStreamReader(req.getInputStream()));
    	StringBuilder sb = new StringBuilder();
    	String tmp;
    	while ((tmp = br.readLine()) != null) {
			sb.append(tmp);
		}
    	
    	String flumeJson = sb.toString();
    	String content = null;
        byte []bs = null;
        content = flumeJson.substring(0, flumeJson.lastIndexOf("\""));
		content = content.substring(content.lastIndexOf("\"")+1);
		bs = BeaverUtils.decompress(content.getBytes(DEFAULT_CHARSET));
		content = new String(bs,DEFAULT_CHARSET);
		System.out.println("content = "+content);
		
		String dbName = null;
		JSONArray newjArray = JSONArray.fromObject(content);
		if(newjArray.size()>0){
			JSONObject job = newjArray.getJSONObject(0);
			dbName = (String) job.get("hdfs_db");
		}
		System.out.println("dbName = " + dbName);

		if(!content.contains("HeartBeat") && map.containsKey(dbName)){
			try {
				updateTask(content, map.get(dbName));
			} catch (ParseException e) {
				e.printStackTrace();
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
							if(Long.parseLong(maxxgsj,16) > Long.parseLong(tBean.getXgsj(),16)){
								tBean.setXgsj(maxxgsj);
								System.out.println("update table："+tBean.getTable()+" "+tBean.getXgsj());
							}
						}
						else if(serverType.equals("webservice")){					
							if(Long.parseLong(maxxgsj) > Long.parseLong(tBean.getStarttime())){
								tBean.setStarttime(maxxgsj);
								System.out.println("update table："+tBean.getTable()+" "+tBean.getStarttime());
							}		
						}
						else if(serverType.equals("oracle")){					
							if(Long.parseLong(maxxgsj) > Long.parseLong(tBean.getID())){
								tBean.setID(maxxgsj);
								System.out.println("update table："+tBean.getTable()+" "+tBean.getID());
							}		
						}
						return;
					}
				}
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
}
