package com.cloudbeaver.mockServer;

import java.io.IOException;
import java.util.Enumeration;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class SimpleTests extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	Enumeration<String> params = req.getParameterNames();
    	while(params.hasMoreElements()){
    		String paramName = params.nextElement();
    		System.out.println(paramName + " " + req.getParameter(paramName));
    	}
    }
}
