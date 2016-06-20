package com.cloudbeaver.server.brokermonitor;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;



import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.*;


class ZKType {
	private String ids;
	@JsonIgnore
	private int jmx_port;
	@JsonIgnore
	private String timestamp;
	@JsonIgnore
	private ArrayList<String> endpoints;
	private String host;
	@JsonIgnore
	private int version;
	private int port;
	
	public ZKType () {}
	
	public String getIds () {
		return ids;
	}
	public void setIds (String x) {
		ids = x;
	}
	public int getJmx_port () {
		return jmx_port;
	}
	public String getTimestamp () {
		return timestamp;
	}
	public ArrayList<String> getEndpoints() {
		return endpoints;
	}
	public String getHost () {
		return host;
	}
	public int getVersion () {
		return version;
	}
	public int getPort () {
		return port;
	}
	/*public String toString () {
		return "lsx [ host: " + host + ", port: " + port + " ]";
	}*/
}

class ResType {
	private int error_code;
	private ArrayList<ZKType> zkd;
	
	public ResType () {
		error_code = 0;
		zkd = new ArrayList<ZKType> ();
	}
	
	public int getError_code () {
		return error_code;
	}
	public void setError_code (int x) {
		error_code = x;
	}
	public ArrayList<ZKType> getZkd () {
		return zkd;
	}
	public void addZkd (ZKType x) {
		zkd.add(x);
	}
	
	/*public String toString () {
		return "Message: {error_code: " + error_code + ", [host: " + this.getHost() + ", port: " + this.getPort() + " ]}";
	}*/
}

class SimpleWatchClient implements Runnable {
	
	public static final String _ERR1 = "KeeperErrorCode = ConnectionLoss for /brokers/ids";
	
	public static final int CLIENT_PORT = 3181;
	public static final String ZNODE = "/brokers/ids"; // The ZNODE to be watched
	private static ZooKeeper zk;
	
	private ZKType Ob_zkdata;
	private static ResType Ob_res;
	
	Watcher wc;
	
	ObjectMapper mapper = new ObjectMapper();
	
	public String GetResult () {
			String Result = new String ();
			try {
				Result = mapper.writeValueAsString(Ob_res);
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
			return Result;
	}

	public void getJsonInfor () {
		Ob_res = new ResType (); // clear, set error_code to 0
				try {
					List<String> watched_nodes = zk.getChildren(ZNODE, true);
					if (watched_nodes.size() == 0) { // No node!
						Ob_res.setError_code(2);
						//System.out.println("No node!");
						return;
					}
					Iterator<String> it_wn = watched_nodes.iterator();
					while (it_wn.hasNext()) {
						String b_id = it_wn.next();
						//System.out.println(b_id);
						String wn_path = ZNODE + "/" + b_id;
						String zkData = new String(zk.getData(wn_path, true, null));
						try {
							//String testJson = "{\"jmx_port\":-1,\"timestamp\":\"1466058881070\",\"endpoints\":[\"PLAINTEXT://localhost:9092\"],\"host\":\"localhost\",\"version\":3,\"port\":9093}\"";
							//Ob_res = mapper.readValue(testJson, ResType.class);
							Ob_zkdata = mapper.readValue(zkData, ZKType.class);
							Ob_zkdata.setIds(b_id);
							Ob_res.addZkd(Ob_zkdata);
						} catch (JsonParseException e) {
					         e.printStackTrace();
					      } catch (JsonMappingException e) {
					         e.printStackTrace();
					      } catch (IOException e) {
					         e.printStackTrace();
					      }
					}
				} catch (Exception e) {
						//e.printStackTrace();
						if (_ERR1.equals(e.getMessage())) {
							//System.out.println("SUC!");
							Ob_res.setError_code(1);
						}
				}
	}
	public SimpleWatchClient () throws IOException {
		// host+port, session timeout, calback
		zk = new ZooKeeper("127.0.0.1:" + CLIENT_PORT, 500000,
			new Watcher() {
				public void process(WatchedEvent event) {}
			});
		// Initial information
		getJsonInfor();
	}
	@Override
	public void run() {
		wc = new Watcher() {
			@Override
			public void process (WatchedEvent event) {
				//System.out.println("----------Start to execute Wathcer.process()----------");
				//System.out.println(event);
				getJsonInfor();
								
				// register a new watcher to keep watching
				try {
					//zk.exists(ZNODE, wc);
					zk.getChildren(ZNODE, wc);
				} catch (KeeperException e1) {
					//e1.printStackTrace();
					if (_ERR1.equals(e1.getMessage())) {
						//System.out.println("SUC2!");
						Ob_res.setError_code(1);
					}
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}
		};
		
		//System.out.println("----------Continuously watching----------");
			try {
				//zk.exists(ZNODE, wc);
				zk.getChildren(ZNODE, wc);
			} catch (KeeperException e1) {
				//e1.printStackTrace();
				if (_ERR1.equals(e1.getMessage())) {
					//System.out.println("SUC2!");
					Ob_res.setError_code(1);
				}
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			// Sleep for a while for reducing pressure on CPU usage
			/*while (true) {		
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
				e.printStackTrace();
				}
		}*/
	}
}




@WebServlet("/zkwatch/")
public class BrokerMonitor extends HttpServlet {
	
	SimpleWatchClient WW;
	public BrokerMonitor() throws Exception {
			super();
			WW = new SimpleWatchClient();
			Thread t = new Thread(WW);
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
    	//pw.write(tableId);
        //pw.write("Beaver欢迎您lsx");
        String docType =
			      "<!doctype html public \"-//w3c//dtd html 4.0 " +
			      "transitional//en\">\n";
        pw.println(docType +
		        "<html>\n" +
		        "<body bgcolor=\"#f0f0f0\">\n" +
		        "<center>" +
		        "<p>" + WW.GetResult()  + "</center>" + "</p>\n");
        
        
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
