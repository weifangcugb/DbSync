package com.cloudbeaver.server.brokermonitor.zkbean;

import java.util.ArrayList;

public class BrokerListBean {
    private int error_code;
    private ArrayList<BrokerZkNodeBean> zkd;
    
    public BrokerListBean () {
    	error_code = 0;
    	zkd = new ArrayList<BrokerZkNodeBean> ();
    }
    
    public int getError_code () {
    	return error_code;
    }
    public void setError_code (int x) {
    	error_code = x;
    }
    public ArrayList<BrokerZkNodeBean> getZkd () {
    	return zkd;
    }
    public void addZkd (BrokerZkNodeBean x) {
    	zkd.add(x);
    }
    
    /*public String toString () {
    	return "Message: {error_code: " + error_code + ", [host: " + this.getHost() + ", port: " + this.getPort() + " ]}";
    }*/
}
