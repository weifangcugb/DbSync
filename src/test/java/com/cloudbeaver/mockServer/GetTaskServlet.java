package com.cloudbeaver.mockServer;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@WebServlet("/api/business/sync/*")
public class GetTaskServlet extends HttpServlet{
	private static String getTaskApi = "/api/business/sync/";

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	String url = req.getRequestURI();
    	int tableIdIndex = url.lastIndexOf('/');
    	if (url.length() <= getTaskApi.length() || tableIdIndex != (getTaskApi.length() - 1)) {
			throw new ServletException("invalid url, format: " + getTaskApi + "{tableId}");
		}

    	String tableId = url.substring(tableIdIndex + 1);
    	ServletOutputStream out = resp.getOutputStream();
    	out.write(tableId.getBytes());
//        out.write("Beaver欢迎您".getBytes());
        out.flush();
//        out.close();
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
