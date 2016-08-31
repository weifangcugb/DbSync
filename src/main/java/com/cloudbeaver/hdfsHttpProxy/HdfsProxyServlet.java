package com.cloudbeaver.hdfsHttpProxy;

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;
import com.cloudbeaver.client.common.BeaverUtils;

@WebServlet("/uploaddata")
public class HdfsProxyServlet extends HttpServlet{
	private static Logger logger = Logger.getLogger(HdfsProxyServlet.class);
	protected boolean EXCEPTION_TEST_MODE = false;
	public static int BUFFER_SIZE = 64 * 1024 * 1024;
	private HdfsHelper hdfsHelper = new HdfsHelper();

	@Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException{
    	String filename = req.getParameter("fileName");
    	byte[] buffer = new byte[BUFFER_SIZE];
    	ServletInputStream servletInputStream = req.getInputStream();
    	int readNumTillNow = 0, len = 0;
    	while((len = servletInputStream.read(buffer, readNumTillNow, BUFFER_SIZE - readNumTillNow)) != -1){
			readNumTillNow += len;
			if(readNumTillNow == BUFFER_SIZE){
				HdfsHelper.writeFile(filename, buffer, readNumTillNow);
				logger.info("upload data, file:" + filename + " size:" + readNumTillNow);

//				clear buffer and read next block
				readNumTillNow = 0;
				BeaverUtils.clearByteArray(buffer);
			}
		}

//    	read to the end of the file
    	if (readNumTillNow > 0) {
    		HdfsHelper.writeFile(filename, buffer, readNumTillNow);
		}
    }
}