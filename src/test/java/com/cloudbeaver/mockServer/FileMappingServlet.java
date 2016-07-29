package com.cloudbeaver.mockServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URLDecoder;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class FileMappingServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException{
    	String tocken = req.getHeader("Authorization");

    	BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(req.getInputStream()));
    	StringBuilder sb = new StringBuilder();
    	String tmp = null;
    	while((tmp = bufferedReader.readLine()) != null){
    		sb.append(tmp);
    	}

    	System.out.println(" tocken:" + tocken + " msg:" + sb.toString());
    	int startIndex = sb.indexOf("\"path\":\"") + "\"path\":\"".length();
    	String newPath = sb.substring(startIndex, sb.indexOf("\"", startIndex));
    	System.out.println(" newPath:" + URLDecoder.decode(newPath));

    	PrintWriter pWriter = new PrintWriter(resp.getOutputStream());
    	pWriter.print(URLDecoder.decode(newPath));
    	pWriter.close();
    }
}
