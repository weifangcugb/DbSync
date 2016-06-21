package com.cloudbeaver.server.brokermonitor;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class BrokerMonitorServlet extends HttpServlet {
	ZookeeperWatcher zkWatcher;
	public BrokerMonitorServlet() throws Exception {
        super();
        zkWatcher = new ZookeeperWatcher();
        Thread t = new Thread(zkWatcher);
        t.start();
	}

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	String url = req.getRequestURI();

    	resp.setCharacterEncoding("GBK");
    	PrintWriter pw = resp.getWriter();
    	pw.print(zkWatcher.GetResult());
        pw.flush();
        pw.close();
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
