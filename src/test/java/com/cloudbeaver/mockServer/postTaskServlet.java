package com.cloudbeaver.mockServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

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

@WebServlet("/")
public class postTaskServlet extends HttpServlet{
	private static Logger logger = Logger.getLogger(postTaskServlet.class);
	private static String getTaskApi = "/";
	public static String DEFAULT_CHARSET = "utf-8";

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
    	
    	updateTask(sb.toString());		

    	int debugLen = sb.length() > 300 ? 300 : sb.length();
    	System.out.println("get post data, data:" + sb.toString().substring(0, debugLen));
    }
    
    public static void updateTask(String flumeJson) throws IOException{
    	MultiDatabaseBean databases = GetTaskServlet.getMultiDatabaseBean();
    	long maxxgsj;
    	Object table = null;
    	Object database = null;
    	String content = null;
        byte []bs = null;
        content = flumeJson.substring(0, flumeJson.lastIndexOf("\""));
		content = content.substring(content.lastIndexOf("\"")+1);
		bs = BeaverUtils.decompress(content.getBytes(DEFAULT_CHARSET));
		content = new String(bs,DEFAULT_CHARSET);
		JSONArray newjArray = JSONArray.fromObject(content);
		if(newjArray.size()>0){
			for(int i=0;i<newjArray.size();i++){
				JSONObject iob = newjArray.getJSONObject(i);
				maxxgsj = Long.parseLong(iob.get("xgsj2").toString());
				table = iob.get("hdfs_table");
				database = iob.get("hdfs_db");
				
				searchOriginTask(databases,database,table,maxxgsj,i);
				
				//GetTaskServlet.setDodumentDBInitJson(databases);
//				System.out.println(GetTaskServlet.getDodumentDBInitJson());
//				logger.info("new task："+GetTaskServlet.getDodumentDBInitJson());
			}
		}
    }
    
    public static void searchOriginTask(MultiDatabaseBean databases,Object database,Object table,long maxxgsj, int i) throws IOException{
		for(int j = 0;j<databases.getDatabases().size();j++){
			DatabaseBean dbBean = databases.getDatabases().get(i);
			if(dbBean.getDb().equals(database)){
				for (TableBean tBean : dbBean.getTables()) {
					if(tBean.getTable().equals(table)){
						if(maxxgsj > Long.parseLong(tBean.getXgsj())){
							tBean.setXgsj(String.valueOf(maxxgsj));
							System.out.println("update table："+tBean.getTable()+" "+tBean.getXgsj());
							return;
						}
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