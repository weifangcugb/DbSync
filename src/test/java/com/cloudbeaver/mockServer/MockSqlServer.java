package com.cloudbeaver.mockServer;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import com.cloudbeaver.client.common.BeaverUtils;

public class MockSqlServer {
	private static Logger logger = Logger.getLogger(MockSqlServer.class);
	
	public void start() throws Exception{	
		File f = new File("testdb.db");
		if(f.exists()){
			logger.info("database exists");
		}
	}
	public void createDatabase(String sqlFile) throws Exception{
		Connection c = null;
		Statement stmt = null;
	    try {  		    	
	    	Class.forName("org.sqlite.JDBC");
	    	c = DriverManager.getConnection("jdbc:sqlite:src/resources/testdb.db");
	    	c.close();
	    } catch ( Exception e ) {
	    	BeaverUtils.PrintStackTrace(e);
	    	logger.error( e.getClass().getName() + ": " + e.getMessage() );
	    }
	    logger.info("create database successfully");
	}
	
	public void execSql() throws Exception{
		Connection c = null;
		Statement stmt = null;
		Statement stmt2 = null;
	    try {
	    	Class.forName("org.sqlite.JDBC");
	    	c = DriverManager.getConnection("jdbc:sqlite:src/resources/testdb.db");
	    	String sql = null;
	    	ResultSet rs = null;
	    	stmt = c.createStatement();
	    	stmt2 = c.createStatement();
	    	sql = "SELECT name FROM sqlite_master WHERE type='table';";	    	
	        rs = stmt.executeQuery( sql);
	        List<String> list = new ArrayList<String>();
	        while ( rs.next() ) {
	        	String name = rs.getString("name");
	        	list.add(name);
	        }
	        System.out.println(list.size());
	        List<String> r = new ArrayList<String>();
	        int count = 0;
	        int a = 0;
	        int b = 0;
	        int d = 0;
	        String temp = null;
	        for(int i = 0;i<list.size();i++){
	        	String name = list.get(i);
//	        	if(name.equals("sqlite_sequence"))
//	        		continue;
	        	sql = "select sql from sqlite_master where tbl_name='"+name+"';";
	        	//System.out.println(sql);
	        	rs = stmt.executeQuery( sql);	        	
	        	while ( rs.next() ) {
	        		String s = rs.getString("sql");
	        		if(s==null)
	        			break;
//	        		if(s.contains("[xgsj]") && s.contains("[id]")){
//	        			sql = "alter table "+name+" add column xgsj2 NUMERIC;";
//	        			stmt.executeUpdate(sql);
//	        			a++;
//	        			r.add(name);
//	        			break;
//	        		}else if(s.contains("[xgsj]") && !s.contains("[id]")){
//	        			b++;
//	        			System.out.println(name);
//	        			break;
//	        		}else if(!s.contains("[xgsj]")){
//	        			System.out.println(name);
//	        			count++;
//	        			break;
//	        		}
	        		if(s.contains("[XGSJ]") && s.contains("[ID]")){
	        			d++;
	        			r.add(name);
//	        			sql = "alter table "+name+" add column xgsj2 NUMERIC;";
//	        			stmt.executeUpdate(sql);
	        			break;
	        		}
	        	}
	        }
	        System.out.println("表的总数:"+list.size());
//	        System.out.println("没有xgsj:"+count);
//	        System.out.println("有xgsj，没有id:"+b);
//	        System.out.println("有xgsj，有id:"+a);
//	        System.out.println("有xgsj，没有id的表是:"+temp);
//	        System.out.println("有xgsj，有id:"+d);
	        
	        System.out.println("r.size() = "+r.size());
	        
//	        String sql2 = null;
//	        for(int i = 0;i<r.size();i++){
//	        	String name = r.get(i);
//	        	sql = "select * from "+ name +";";
//	        	System.out.println(sql);
//	        	rs = stmt.executeQuery( sql);
//	        	while ( rs.next() ) {
//	        		int id = rs.getInt("ID");
//		        	byte [] bytes = rs.getBytes("XGSJ");
//		        	if(bytes != null){
//		        		long l = toLong(bytes);
//			        	sql2 = "update "+name+" set xgsj2 = "+l+" where ID = "+id+";";
//			        	System.out.println(sql2);
//			        	stmt2.executeUpdate(sql2);
//		        	}	        			        	
//		        }
//	        }
    
        
//	        sql = "select * from DA_JBXX;";
//	        System.out.println(sql);
//	        rs = stmt.executeQuery(sql);
//	        ResultSetMetaData m=rs.getMetaData();	        
//	        int columns=m.getColumnCount();
//
//	        while ( rs.next() ) {
//	        	byte [] bytes = rs.getBytes("xgsj");
//	        	long l = rs.getLong("xgsj2");
//	        	System.out.println(bytes+" "+l);
//	        }
	        rs.close();
	        stmt.close();	    	
	    	c.close();
	    } catch ( Exception e ) {
	    	BeaverUtils.PrintStackTrace(e);
	    	logger.error( e.getClass().getName() + ": " + e.getMessage() );
	    }
	    logger.info("query successfully");
	}
	
	public static long toLong(byte[] b) {
        long s = 0;
        long s7 = b[0] & 0xff;// 最低位
        long s6 = b[1] & 0xff;
        long s5 = b[2] & 0xff;
        long s4 = b[3] & 0xff;
        long s3 = b[4] & 0xff;// 最低位
        long s2 = b[5] & 0xff;
        long s1 = b[6] & 0xff;
        long s0 = b[7] & 0xff;
        // s0不变
        s1 <<= 8;
        s2 <<= 16;
        s3 <<= 24;
        s4 <<= 8 * 4;
        s5 <<= 8 * 5;
        s6 <<= 8 * 6;
        s7 <<= 8 * 7;
        s = s0 | s1 | s2 | s3 | s4 | s5 | s6 | s7;
        return s;
    }


	public void stop(){
		File f = new File("testdb.db");
		if(f.exists()){
			f.delete();
			logger.info("delete database successfully");
		}
	}
	
	public static void main(String[] args) throws Exception{
		MockSqlServer mockSqlServer = new MockSqlServer();
//		mockSqlServer.start();
		mockSqlServer.execSql();
//		mockSqlServer.stop();
	}
}
