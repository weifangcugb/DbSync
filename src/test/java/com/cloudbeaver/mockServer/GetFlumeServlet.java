package com.cloudbeaver.mockServer;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;


@WebServlet("/")
public class GetFlumeServlet extends HttpServlet{
	private static Logger logger = Logger.getLogger(GetFlumeServlet.class);
	private static String getTaskApi = "/";

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
    	System.out.println("get flume succeed!");
    	//resp.setHeader(\"Content-type\", \"text/html;charset=UTF-8\");
//    	resp.setCharacterEncoding("utf-8");
//    	PrintWriter pw = resp.getWriter();
//    	
//      pw.flush();
//      pw.close();
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
