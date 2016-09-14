package com.cloudbeaver.mockServer;

import java.io.IOException;
import java.io.PrintWriter;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;

import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.client.dbUploader.DbUploader;

@WebServlet("/interface/*")
public class GetWebServerDataServlet extends HttpServlet{
	private static Logger logger = Logger.getLogger(GetWebServerDataServlet.class);
	private static String getTaskApi = "/interface/";
	
	@Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException{
//    	Enumeration<String> params = req.getParameterNames();
//    	while(params.hasMoreElements()){
//    		String paramName = params.nextElement();
//    		System.out.println(paramName + " " + req.getParameter(paramName));
//    	}
    	
    	String url = req.getRequestURI();
    	int pos = url.indexOf(getTaskApi);
    	if (pos == -1) {
			throw new ServletException("invalid url: " + url);
		}
    	String tablename = url.substring(pos+getTaskApi.length());
    	System.out.println(tablename);
    	
    	String appkey = req.getParameter("appkey");
    	String starttime = req.getParameter("starttime");
    	String endtime = req.getParameter("endtime");
    	String sign = req.getParameter("sign");
    	String pagesize = req.getParameter("pagesize");
    	
    	int totalsize = 100;
    	if(starttime != null && starttime.equals(GetTaskServlet.fourDayBeforeString)){
    		totalsize = 0;
    	}
    	int totalpages = 0;
    	if(totalsize%Integer.parseInt(pagesize) == 0){
    		totalpages = totalsize/Integer.parseInt(pagesize);
    	}
    	else{
    		totalpages = totalsize/Integer.parseInt(pagesize)+1;
    	}
    	String pageno = null;   
    	if(req.getParameter("pageno") == null){
    		if(totalpages == 0){
    			pageno = "0";
    		}
    		else{
    			pageno = "1";
    		}
    	}
    	else{
    		pageno = req.getParameter("pageno");
    	}
//    	System.out.println("pageno = "+pageno);
    	if(!DbUploader.getAppKeySecret().containsKey(appkey)){
    		System.out.println(appkey);
    		throw new ServletException("AppKey is invalid!");
    	}
    	String originSign = null;
		try {
			originSign = createSign(appkey, DbUploader.getAppKeySecret().get(appkey), tablename, pageno, pagesize, starttime, endtime);
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	if(!sign.equals(originSign)){
    		throw new ServletException("Sign is invalid!");
    	}    	
    	
    	String json = getJson(tablename,pagesize,pageno,totalsize,totalpages);
		
    	resp.setHeader("Content-type", "text/html;charset=UTF-8");
//    	resp.setCharacterEncoding("utf-8");
    	PrintWriter pw = resp.getWriter();
        pw.write(json);
        pw.flush();
        pw.close();
    }
    
    public static String getJson(String tablename, String pagesize, String pageno, int totalsize, int totalpages){
    	String json = "";
    	if(tablename.equals("pias/getItlist")){
    		json = "{\"pageNo\":" + pageno + ",\"pageSize\":" + pagesize + ",\"records\":[{\"bj\":0,\"children\":0,\"endtime\":1384741859453,\"entertime\":1384739540593,"
    				+"\"exittime\":1384738423000,\"id\":121,\"itlocation\":\"会见楼6号卡位\",\"itlx\":\"普通会见\",\"itsibs\":"
    				+"[{\"cardno\":\"6444445\",\"cardtype\":\"\",\"dwmx\":\"xxxx街道\",\"id\":161,\"jtmx\":\"xx卫xx县xx小区12号\",\"jtzz\":\" \","
    				+"\"name\":\"王二\",\"parentid\":121,\"phone\":\"\",\"qw\":\"父亲\",\"sex\":\"\",\"sibid\":\"13440\",\"zy\":\"农民\"},{\"cardno\":\"\","
    				+"\"cardtype\":\"\",\"dwmx\":\"\",\"id\":162,\"jtmx\":\"\",\"jtzz\":\" \",\"name\":\"王元\",\"parentid\":121,\"phone\":\"\",\"qw\":\"叔父\","
    				+"\"sex\":\"\",\"sibid\":\"315\",\"zy\":\"\"},{\"cardno\":\"\",\"cardtype\":\"\",\"dwmx\":\"\",\"id\":163,\"jtmx\":\"\",\"jtzz\":\" \","
    				+"\"name\":\"王凤\",\"parentid\":121,\"phone\":\"\",\"qw\":\"婶婶\",\"sex\":\"\",\"sibid\":\"312\",\"zy\":\"\"}],\"ittime\":1384738423220,"
    				+ "\"ittype\":\"普通会见\",\"lasttime\":\"\",\"leadtime\":1384739504137,\"ls\":0,\"normal\":2,\"noticetime\":1384739504137,\"peoples\":3,"
    				+ "\"pfcardno\":\"\",\"pfcardtype\":\"身份证\",\"pfname\":\"王年\",\"printtime\":1384738423220,\"qiq\":0,\"regtime\":1384738423220,"
    				+ "\"reviewtime\":0,\"starttime\":1384739540593,\"syprisonfk\":\"612222\",\"thismonth\":0,\"thisyear\":2,\"ts\":0},{\"bj\":0,\"children\":0,"
    				+ "\"endtime\":1384743586667,\"entertime\":1384739404733,\"exittime\":1384738549000,\"id\":122,\"itlocation\":\"会见楼24号卡位\",\"itlx\":\"普通会见\","
    				+ "\"itsibs\":[{\"cardno\":\"62012313\",\"cardtype\":\"\",\"dwmx\":\"\",\"id\":165,\"jtmx\":\"xx市xx区66号\",\"jtzz\":\" \",\"mm\":\"群众\","
    				+ "\"name\":\"刘杨\",\"parentid\":122,\"phone\":\"\",\"qw\":\"母亲\",\"sex\":\"\",\"sibid\":\"4241\",\"zy\":\"个体\"},{\"cardno\":\"64010319540123151X\","
    				+ "\"cardtype\":\"\",\"dwmx\":\"\",\"id\":164,\"jtmx\":\"xx市xx区66号\",\"jtzz\":\" \",\"mm\":\"群众\",\"name\":\"李平\",\"parentid\":122,\"phone\":\"\","
    				+ "\"qw\":\"父亲\",\"sex\":\"\",\"sibid\":\"412\",\"zy\":\"个体\"}],\"ittime\":1384738549083,\"ittype\":\"普通会见\",\"lasttime\":\"\",\"leadtime\":1384741573800,"
    				+ "\"ls\":0,\"normal\":0,\"noticetime\":1384741573800,\"peoples\":2,\"pfcardno\":\"\",\"pfcardtype\":\"身份证\",\"pfname\":\"李东\",\"printtime\":1384738549083,"
    				+ "\"qiq\":0,\"regtime\":1384738549083,\"reviewtime\":0,\"starttime\":1384739404733,\"syprisonfk\":\"615564\",\"thismonth\":0,\"thisyear\":0,\"ts\":0}],"
    				+ "\"total\":" + totalsize +",\"totalPages\":" + totalpages + "}";
//    		json = "{\"pageNo\":" + pageno + ",\"pageSize\":" + pagesize + ",\"records\":[{\"bj\":0,\"children\":0,\"endtime\":1384741859453,\"entertime\":1384739540593,"
//    				+"\"exittime\":1384738423000,\"id\":121,\"itlocation\":\"会见楼6号卡位\",\"itlx\":\"普通会见\",\"itsibs\":"
//    				+"[{\"cardno\":\"6444445\",\"cardtype\":\"\",\"dwmx\":\"xxxx街道\",\"id\":161,\"jtmx\":\"xx卫xx县xx小区12号\",\"jtzz\":\" \","
//    				+"\"name\":\"王二\",\"parentid\":121,\"phone\":\"\",\"qw\":\"父亲\",\"sex\":\"\",\"sibid\":\"13440\",\"zy\":\"农民\"},{\"cardno\":\"\","
//    				+"\"cardtype\":\"\",\"dwmx\":\"\",\"id\":162,\"jtmx\":\"\",\"jtzz\":\" \",\"name\":\"王元\",\"parentid\":121,\"phone\":\"\",\"qw\":\"叔父\","
//    				+"\"sex\":\"\",\"sibid\":\"315\",\"zy\":\"\"},{\"cardno\":\"\",\"cardtype\":\"\",\"dwmx\":\"\",\"id\":163,\"jtmx\":\"\",\"jtzz\":\" \","
//    				+"\"name\":\"王凤\",\"parentid\":121,\"phone\":\"\",\"qw\":\"婶婶\",\"sex\":\"\",\"sibid\":\"312\",\"zy\":\"\"}],\"ittime\":1384738423220,"
//    				+ "\"ittype\":\"普通会见\",\"lasttime\":\"\",\"leadtime\":1384739504137,\"ls\":0,\"normal\":2,\"noticetime\":1384739504137,\"peoples\":3,"
//    				+ "\"pfcardno\":\"\",\"pfcardtype\":\"身份证\",\"pfname\":\"王年\",\"printtime\":1384738423220,\"qiq\":0,\"regtime\":1384738423220,"
//    				+ "\"reviewtime\":0,\"starttime\":1484739540593,\"syprisonfk\":\"612222\",\"thismonth\":0,\"thisyear\":2,\"ts\":0},{\"bj\":0,\"children\":0,"
//    				+ "\"endtime\":1384743586667,\"entertime\":1384739404733,\"exittime\":1384738549000,\"id\":122,\"itlocation\":\"会见楼24号卡位\",\"itlx\":\"普通会见\","
//    				+ "\"itsibs\":[{\"cardno\":\"62012313\",\"cardtype\":\"\",\"dwmx\":\"\",\"id\":165,\"jtmx\":\"xx市xx区66号\",\"jtzz\":\" \",\"mm\":\"群众\","
//    				+ "\"name\":\"刘杨\",\"parentid\":122,\"phone\":\"\",\"qw\":\"母亲\",\"sex\":\"\",\"sibid\":\"4241\",\"zy\":\"个体\"},{\"cardno\":\"64010319540123151X\","
//    				+ "\"cardtype\":\"\",\"dwmx\":\"\",\"id\":164,\"jtmx\":\"xx市xx区66号\",\"jtzz\":\" \",\"mm\":\"群众\",\"name\":\"李平\",\"parentid\":122,\"phone\":\"\","
//    				+ "\"qw\":\"父亲\",\"sex\":\"\",\"sibid\":\"412\",\"zy\":\"个体\"}],\"ittime\":1384738549083,\"ittype\":\"普通会见\",\"lasttime\":\"\",\"leadtime\":1384741573800,"
//    				+ "\"ls\":0,\"normal\":0,\"noticetime\":1384741573800,\"peoples\":2,\"pfcardno\":\"\",\"pfcardtype\":\"身份证\",\"pfname\":\"李东\",\"printtime\":1384738549083,"
//    				+ "\"qiq\":0,\"regtime\":1384738549083,\"reviewtime\":0,\"starttime\":1484739404733,\"syprisonfk\":\"615564\",\"thismonth\":0,\"thisyear\":0,\"ts\":0}],"
//    				+ "\"total\":" + totalsize +",\"totalPages\":" + totalpages + "}";
    	}
    	else if(tablename.equals("qqdh/getTalklist")){
//    		json = "{\"pageNo\":" + pageno + ",\"pageSize\":" + pagesize + ",\"records\":[{\"caller\":\"张三\",\"enddate\":\"2015-04-15 11:11:45\",\"fee\":20.0000,\"id\":7,"
//    				+ "\"module\":1,\"name\":\"张\",\"phone\":\"18912345678\",\"startdate\":\"2015-04-15 11:11:40\",\"state\":1,\"type\":\"拨出\",\"userno\":\"9999999999\"},"
//    				+ "{\"caller\":\"张三\",\"enddate\":\"2015-04-15 11:27:25\",\"fee\":0.0000,\"id\":8,\"module\":1,\"name\":\"张\",\"phone\":\"18912345678\","
//    				+ "\"startdate\":\"2015-04-15 11:27:17\",\"state\":1,\"type\":\"拨出\",\"userno\":\"9999999999\"},{\"caller\":\"张三\",\"enddate\":\"2015-04-15 16:35:46\","
//    				+ "\"fee\":0.0000,\"id\":9,\"module\":1,\"name\":\"张\",\"phone\":\"18912345678\",\"startdate\":\"2015-04-15 16:34:33\",\"state\":1,\"type\":\"拨出\","
//    				+ "\"userno\":\"9999999999\"}],\"total\":" + totalsize +",\"totalPages\":" + totalpages + "}";
    		json = "{\"pageNo\":" + pageno + ",\"pageSize\":" + pagesize + ",\"records\":[{\"caller\":\"张三\",\"enddate\":\"2015-04-15 11:11:45\",\"fee\":20.0000,\"id\":7,"
    				+ "\"module\":1,\"name\":\"张\",\"phone\":\"18912345678\",\"startdate\":\"2016-07-15 11:11:40\",\"state\":1,\"type\":\"拨出\",\"userno\":\"9999999999\"},"
    				+ "{\"caller\":\"张三\",\"enddate\":\"2015-04-15 11:27:25\",\"fee\":0.0000,\"id\":8,\"module\":1,\"name\":\"张\",\"phone\":\"18912345678\","
    				+ "\"startdate\":\"2015-04-15 11:27:17\",\"state\":1,\"type\":\"拨出\",\"userno\":\"9999999999\"},{\"caller\":\"张三\",\"enddate\":\"2015-04-15 16:35:46\","
    				+ "\"fee\":0.0000,\"id\":9,\"module\":1,\"name\":\"张\",\"phone\":\"18912345678\",\"startdate\":\"2016-07-15 16:34:33\",\"state\":1,\"type\":\"拨出\","
    				+ "\"userno\":\"9999999999\"}],\"total\":" + totalsize +",\"totalPages\":" + totalpages + "}";
    	}
    	else if(tablename.equals("qqdh/getQqdh")){
    		json = "{\"pageNo\":" + pageno + ",\"pageSize\":" + pagesize + ",\"records\":[{\"id\":1,\"modifydate\":1413388800000,\"phone\":\"1018\",\"phoneid\":1,"
    				+ "\"relation\":\"1\",\"sibname\":\"张大\",\"userno\":\"0001\"},{\"id\":2,\"modifydate\":\"2016-01-02 12:31:31\",\"phone\":\"18900000000\","
    				+ "\"phoneid\":1,\"relation\":\"父亲\",\"sibname\":\"张三\",\"userno\":\"9999999999\"}],\"total\":" + totalsize +",\"totalPages\":" + totalpages + "}";
    	}
    	else if(tablename.equals("pras/getResult")){
    		json = "{\"pageNo\":" + pageno + ",\"pageSize\":" + pagesize + ",\"records\":[{\"ANSWER\":\"2|1|1|2|1|1|2|2|2|2|1|1|2|2|2|1|2|1|1|2|1|2|1|1|1|2|1|1|1|2|1|2|1|2|1|"
    				+ "1|1|1|1|1|2|1|2|2|1|2|1|2|2|1|1|1|1|2|2|1|1|2|1|1|1|1|2|2|2|1|2|2|1|1|1|1|1|1|2|2|1|2|1|1|2|1|2|2|1|1|1|1\",\"CHECKDAY\":\"2016-01-05 10:33:40\","
    				+ "\"ID\":\"ID00000000020751\",\"OPERATOR\":\"王二\",\"PFCODE\":\"6402512870\",\"PFNAME\":\"李高\",\"PFOUCODE\":\"0101-B1\",\"PFOUNAME\":\"xx监狱十监区\","
    				+ "\"RESULT\":[{\"NAME\":\"jsz_o\",\"NAMECHINESE\":\"精神质（p）\",\"ORIGIN\":\"4.0\",\"SPECIALDOCTOR\":\"7.1.3\",\"STANDARD\":\"45.0\"},"
    				+ "{\"NAME\":\"nwx_o\",\"NAMECHINESE\":\"内外向（e）\",\"ORIGIN\":\"8.0\",\"SPECIALDOCTOR\":\"7.2.3\",\"STANDARD\":\"45.0\"},"
    				+ "{\"NAME\":\"sjz_o\",\"NAMECHINESE\":\"神经质（n）\",\"ORIGIN\":\"18.0\",\"SPECIALDOCTOR\":\"7.3.1\",\"STANDARD\":\"65.0\"},"
    				+ "{\"NAME\":\"yscd_o\",\"NAMECHINESE\":\"掩饰程度（l）\",\"ORIGIN\":\"9.0\",\"SPECIALDOCTOR\":\"7.4.4\",\"STANDARD\":\"40.0\"}],"
    				+ "\"SAVETABLE\":\"Subepq\",\"SEX\":\"男\",\"SQLROWSETDATA_NUM\":1,\"SUBTABLEID\":78120,\"SUMMARY\":\"该犯属中间型性格，情绪不稳定，易焦虑、紧张，易与他人发生冲突，"
    				+ "管理中需注意防止违规违纪现象发生。\",\"TABLECODE\":\"7\",\"TABLENAME\":\"艾森克个性测验(成人)(Epq)\",\"TABLENAMEENGLISH\":\"Epq\",\"TESTCODE\":\"ID00000000020751\"}],"
    				+ "\"total\":" + totalsize +",\"totalPages\":" + totalpages + "}";
    	}
    	else if(tablename.equals("pras/getTable")){
    		json = "{\"pageNo\":" + pageno + ",\"pageSize\":" + pagesize + ",\"records\":[{\"CODE\":\"1\",\"GUIDE\":\"以下每个题目都有一定的主题，但是每张大的主题图中都缺少一部分，"
    				+ "主题图以下有6—8张小图片，若填补在主题图的缺失部分，可以使整个图案合理与完整，请从每题下面所给出的小图片中找出适合大图案的一张。\",\"ID\":\"1\",\"ISLOADWEB\":0,\"ISUSED\":1,"
    				+ "\"NAMECHINESE\":\"瑞文标准推理测验\",\"NAMEENGLISH\":\"Rw\",\"QUESTION\":[{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\","
    				+ "\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWERSUM\":6,\"ID\":\"1\",\"QUESTIONCODE\":1,\"QUESTIONCONTENT\":\"Rw/1.BMP\",\"TABLECODE\":\"1\"},"
    				+ "{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWERSUM\":6,\"ID\":\"2\","
    				+ "\"QUESTIONCODE\":2,\"QUESTIONCONTENT\":\"Rw/2.BMP\",\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\","
    				+ "\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWERSUM\":6,\"ID\":\"3\",\"QUESTIONCODE\":3,\"QUESTIONCONTENT\":\"Rw/3.BMP\",\"TABLECODE\":\"1\"},"
    				+ "{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWERSUM\":6,\"ID\":\"4\","
    				+ "\"QUESTIONCODE\":4,\"QUESTIONCONTENT\":\"Rw/4.BMP\",\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\","
    				+ "\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWERSUM\":6,\"ID\":\"5\",\"QUESTIONCODE\":5,\"QUESTIONCONTENT\":\"Rw/5.BMP\",\"TABLECODE\":\"1\"},"
    				+ "{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWERSUM\":6,\"ID\":\"6\","
    				+ "\"QUESTIONCODE\":6,\"QUESTIONCONTENT\":\"Rw/6.BMP\",\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\","
    				+ "\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWERSUM\":6,\"ID\":\"7\",\"QUESTIONCODE\":7,\"QUESTIONCONTENT\":\"Rw/7.BMP\",\"TABLECODE\":\"1\"},"
    				+ "{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWERSUM\":6,\"ID\":\"8\","
    				+ "\"QUESTIONCODE\":8,\"QUESTIONCONTENT\":\"Rw/8.BMP\",\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\","
    				+ "\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWERSUM\":6,\"ID\":\"9\",\"QUESTIONCODE\":9,\"QUESTIONCONTENT\":\"Rw/9.BMP\",\"TABLECODE\":\"1\"},"
    				+ "{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWERSUM\":6,\"ID\":\"10\","
    				+ "\"QUESTIONCODE\":10,\"QUESTIONCONTENT\":\"Rw/10.BMP\",\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\","
    				+ "\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWERSUM\":6,\"ID\":\"11\",\"QUESTIONCODE\":11,\"QUESTIONCONTENT\":\"Rw/11.BMP\",\"TABLECODE\":\"1\"},"
    				+ "{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWERSUM\":6,\"ID\":\"12\","
    				+ "\"QUESTIONCODE\":12,\"QUESTIONCONTENT\":\"Rw/12.BMP\",\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\","
    				+ "\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWERSUM\":6,\"ID\":\"13\",\"QUESTIONCODE\":13,\"QUESTIONCONTENT\":\"Rw/13.BMP\",\"TABLECODE\":\"1\"},"
    				+ "{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWERSUM\":6,\"ID\":\"14\","
    				+ "\"QUESTIONCODE\":14,\"QUESTIONCONTENT\":\"Rw/14.BMP\",\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\","
    				+ "\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWERSUM\":6,\"ID\":\"15\",\"QUESTIONCODE\":15,\"QUESTIONCONTENT\":\"Rw/15.BMP\",\"TABLECODE\":\"1\"},"
    				+ "{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWERSUM\":6,\"ID\":\"16\","
    				+ "\"QUESTIONCODE\":16,\"QUESTIONCONTENT\":\"Rw/16.BMP\",\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\","
    				+ "\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWERSUM\":6,\"ID\":\"17\",\"QUESTIONCODE\":17,\"QUESTIONCONTENT\":\"Rw/17.BMP\",\"TABLECODE\":\"1\"},"
    				+ "{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWERSUM\":6,\"ID\":\"18\","
    				+ "\"QUESTIONCODE\":18,\"QUESTIONCONTENT\":\"Rw/18.BMP\",\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\","
    				+ "\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWERSUM\":6,\"ID\":\"19\",\"QUESTIONCODE\":19,\"QUESTIONCONTENT\":\"Rw/19.BMP\",\"TABLECODE\":\"1\"},"
    				+ "{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWERSUM\":6,\"ID\":\"20\","
    				+ "\"QUESTIONCODE\":20,\"QUESTIONCONTENT\":\"Rw/20.BMP\",\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\","
    				+ "\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWERSUM\":6,\"ID\":\"21\",\"QUESTIONCODE\":21,\"QUESTIONCONTENT\":\"Rw/21.BMP\",\"TABLECODE\":\"1\"},"
    				+ "{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWERSUM\":6,\"ID\":\"22\","
    				+ "\"QUESTIONCODE\":22,\"QUESTIONCONTENT\":\"Rw/22.BMP\",\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\","
    				+ "\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWERSUM\":6,\"ID\":\"23\",\"QUESTIONCODE\":23,\"QUESTIONCONTENT\":\"Rw/23.BMP\",\"TABLECODE\":\"1\"},"
    				+ "{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWERSUM\":6,\"ID\":\"24\","
    				+ "\"QUESTIONCODE\":24,\"QUESTIONCONTENT\":\"Rw/24.BMP\",\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\","
    				+ "\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWER7\":\"7\",\"ANSWER8\":\"8\",\"ANSWERSUM\":8,\"ID\":\"25\",\"QUESTIONCODE\":25,\"QUESTIONCONTENT\":\"Rw/25.BMP\","
    				+ "\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWER7\":\"7\","
    				+ "\"ANSWER8\":\"8\",\"ANSWERSUM\":8,\"ID\":\"26\",\"QUESTIONCODE\":26,\"QUESTIONCONTENT\":\"Rw/26.BMP\",\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\","
    				+ "\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWER7\":\"7\",\"ANSWER8\":\"8\",\"ANSWERSUM\":8,\"ID\":\"27\","
    				+ "\"QUESTIONCODE\":27,\"QUESTIONCONTENT\":\"Rw/27.BMP\",\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\","
    				+ "\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWER7\":\"7\",\"ANSWER8\":\"8\",\"ANSWERSUM\":8,\"ID\":\"28\",\"QUESTIONCODE\":28,\"QUESTIONCONTENT\":\"Rw/28.BMP\","
    				+ "\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWER7\":\"7\","
    				+ "\"ANSWER8\":\"8\",\"ANSWERSUM\":8,\"ID\":\"29\",\"QUESTIONCODE\":29,\"QUESTIONCONTENT\":\"Rw/29.BMP\",\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\","
    				+ "\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWER7\":\"7\",\"ANSWER8\":\"8\",\"ANSWERSUM\":8,\"ID\":\"30\","
    				+ "\"QUESTIONCODE\":30,\"QUESTIONCONTENT\":\"Rw/30.BMP\",\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\","
    				+ "\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWER7\":\"7\",\"ANSWER8\":\"8\",\"ANSWERSUM\":8,\"ID\":\"31\",\"QUESTIONCODE\":31,\"QUESTIONCONTENT\":\"Rw/31.BMP\","
    				+ "\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWER7\":\"7\","
    				+ "\"ANSWER8\":\"8\",\"ANSWERSUM\":8,\"ID\":\"32\",\"QUESTIONCODE\":32,\"QUESTIONCONTENT\":\"Rw/32.BMP\",\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\","
    				+ "\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWER7\":\"7\",\"ANSWER8\":\"8\",\"ANSWERSUM\":8,\"ID\":\"33\",\"QUESTIONCODE\":33,"
    				+ "\"QUESTIONCONTENT\":\"Rw/33.BMP\",\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\","
    				+ "\"ANSWER6\":\"6\",\"ANSWER7\":\"7\",\"ANSWER8\":\"8\",\"ANSWERSUM\":8,\"ID\":\"34\",\"QUESTIONCODE\":34,\"QUESTIONCONTENT\":\"Rw/34.BMP\",\"TABLECODE\":\"1\"},"
    				+ "{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWER7\":\"7\",\"ANSWER8\":\"8\",\"ANSWERSUM\":8,"
    				+ "\"ID\":\"35\",\"QUESTIONCODE\":35,\"QUESTIONCONTENT\":\"Rw/35.BMP\",\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\","
    				+ "\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWER7\":\"7\",\"ANSWER8\":\"8\",\"ANSWERSUM\":8,\"ID\":\"36\",\"QUESTIONCODE\":36,\"QUESTIONCONTENT\":\"Rw/36.BMP\","
    				+ "\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWER7\":\"7\","
    				+ "\"ANSWER8\":\"8\",\"ANSWERSUM\":8,\"ID\":\"37\",\"QUESTIONCODE\":37,\"QUESTIONCONTENT\":\"Rw/37.BMP\",\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\","
    				+ "\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWER7\":\"7\",\"ANSWER8\":\"8\",\"ANSWERSUM\":8,\"ID\":\"38\",\"QUESTIONCODE\":38,"
    				+ "\"QUESTIONCONTENT\":\"Rw/38.BMP\",\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\","
    				+ "\"ANSWER6\":\"6\",\"ANSWER7\":\"7\",\"ANSWER8\":\"8\",\"ANSWERSUM\":8,\"ID\":\"39\",\"QUESTIONCODE\":39,\"QUESTIONCONTENT\":\"Rw/39.BMP\",\"TABLECODE\":\"1\"},"
    				+ "{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWER7\":\"7\",\"ANSWER8\":\"8\","
    				+ "\"ANSWERSUM\":8,\"ID\":\"40\",\"QUESTIONCODE\":40,\"QUESTIONCONTENT\":\"Rw/40.BMP\",\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\","
    				+ "\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWER7\":\"7\",\"ANSWER8\":\"8\",\"ANSWERSUM\":8,\"ID\":\"41\",\"QUESTIONCODE\":41,"
    				+ "\"QUESTIONCONTENT\":\"Rw/41.BMP\",\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\","
    				+ "\"ANSWER6\":\"6\",\"ANSWER7\":\"7\",\"ANSWER8\":\"8\",\"ANSWERSUM\":8,\"ID\":\"42\",\"QUESTIONCODE\":42,\"QUESTIONCONTENT\":\"Rw/42.BMP\",\"TABLECODE\":\"1\"},"
    				+ "{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWER7\":\"7\",\"ANSWER8\":\"8\","
    				+ "\"ANSWERSUM\":8,\"ID\":\"43\",\"QUESTIONCODE\":43,\"QUESTIONCONTENT\":\"Rw/43.BMP\",\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\","
    				+ "\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWER7\":\"7\",\"ANSWER8\":\"8\",\"ANSWERSUM\":8,\"ID\":\"44\","
    				+ "\"QUESTIONCODE\":44,\"QUESTIONCONTENT\":\"Rw/44.BMP\",\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\","
    				+ "\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWER7\":\"7\",\"ANSWER8\":\"8\",\"ANSWERSUM\":8,\"ID\":\"45\",\"QUESTIONCODE\":45,\"QUESTIONCONTENT\":\"Rw/45.BMP\","
    				+ "\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWER7\":\"7\","
    				+ "\"ANSWER8\":\"8\",\"ANSWERSUM\":8,\"ID\":\"46\",\"QUESTIONCODE\":46,\"QUESTIONCONTENT\":\"Rw/46.BMP\",\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\","
    				+ "\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWER7\":\"7\",\"ANSWER8\":\"8\",\"ANSWERSUM\":8,"
    				+ "\"ID\":\"47\",\"QUESTIONCODE\":47,\"QUESTIONCONTENT\":\"Rw/47.BMP\",\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\","
    				+ "\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWER7\":\"7\",\"ANSWER8\":\"8\",\"ANSWERSUM\":8,\"ID\":\"48\",\"QUESTIONCODE\":48,"
    				+ "\"QUESTIONCONTENT\":\"Rw/48.BMP\",\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\","
    				+ "\"ANSWER6\":\"6\",\"ANSWER7\":\"7\",\"ANSWER8\":\"8\",\"ANSWERSUM\":8,\"ID\":\"49\",\"QUESTIONCODE\":49,\"QUESTIONCONTENT\":\"Rw/49.BMP\",\"TABLECODE\":\"1\"},"
    				+ "{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWER7\":\"7\",\"ANSWER8\":\"8\","
    				+ "\"ANSWERSUM\":8,\"ID\":\"50\",\"QUESTIONCODE\":50,\"QUESTIONCONTENT\":\"Rw/50.BMP\",\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\","
    				+ "\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWER7\":\"7\",\"ANSWER8\":\"8\",\"ANSWERSUM\":8,\"ID\":\"51\","
    				+ "\"QUESTIONCODE\":51,\"QUESTIONCONTENT\":\"Rw/51.BMP\",\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\","
    				+ "\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWER7\":\"7\",\"ANSWER8\":\"8\",\"ANSWERSUM\":8,\"ID\":\"52\",\"QUESTIONCODE\":52,\"QUESTIONCONTENT\":\"Rw/52.BMP\","
    				+ "\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWER7\":\"7\","
    				+ "\"ANSWER8\":\"8\",\"ANSWERSUM\":8,\"ID\":\"53\",\"QUESTIONCODE\":53,\"QUESTIONCONTENT\":\"Rw/53.BMP\",\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\","
    				+ "\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWER7\":\"7\",\"ANSWER8\":\"8\",\"ANSWERSUM\":8,"
    				+ "\"ID\":\"54\",\"QUESTIONCODE\":54,\"QUESTIONCONTENT\":\"Rw/54.BMP\",\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\","
    				+ "\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWER7\":\"7\",\"ANSWER8\":\"8\",\"ANSWERSUM\":8,\"ID\":\"55\",\"QUESTIONCODE\":55,"
    				+ "\"QUESTIONCONTENT\":\"Rw/55.BMP\",\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\","
    				+ "\"ANSWER6\":\"6\",\"ANSWER7\":\"7\",\"ANSWER8\":\"8\",\"ANSWERSUM\":8,\"ID\":\"56\",\"QUESTIONCODE\":56,\"QUESTIONCONTENT\":\"Rw/56.BMP\","
    				+ "\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWER7\":\"7\","
    				+ "\"ANSWER8\":\"8\",\"ANSWERSUM\":8,\"ID\":\"57\",\"QUESTIONCODE\":57,\"QUESTIONCONTENT\":\"Rw/57.BMP\",\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\","
    				+ "\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWER7\":\"7\",\"ANSWER8\":\"8\",\"ANSWERSUM\":8,\"ID\":\"58\","
    				+ "\"QUESTIONCODE\":58,\"QUESTIONCONTENT\":\"Rw/58.BMP\",\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\","
    				+ "\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWER7\":\"7\",\"ANSWER8\":\"8\",\"ANSWERSUM\":8,\"ID\":\"59\",\"QUESTIONCODE\":59,\"QUESTIONCONTENT\":\"Rw/59.BMP\","
    				+ "\"TABLECODE\":\"1\"},{\"ANSWER1\":\"1\",\"ANSWER2\":\"2\",\"ANSWER3\":\"3\",\"ANSWER4\":\"4\",\"ANSWER5\":\"5\",\"ANSWER6\":\"6\",\"ANSWER7\":\"7\","
    				+ "\"ANSWER8\":\"8\",\"ANSWERSUM\":8,\"ID\":\"60\",\"QUESTIONCODE\":60,\"QUESTIONCONTENT\":\"Rw/60.BMP\",\"TABLECODE\":\"1\"}],\"QUESTIONSUM\":60,"
    				+ "\"SAVETABLE\":\"Subrw\",\"SORTCODE\":83,\"SQLROWSETDATA_NUM\":1}],\"total\":" + totalsize +",\"totalPages\":" + totalpages + "}"; 
    	}
    	return json;
    }

    public static String createSign(String appkey, String appsecret, String tablename, String pageno, String pagesize, String starttime, String endtime) throws NoSuchAlgorithmException{
    	Map<String, String> paraMap = new HashMap<String, String>();
		paraMap.put("appkey", appkey);

		if (Integer.parseInt(pageno) != 1 && Integer.parseInt(pageno) != 0) {
			paraMap.put("pageno", pageno);
		}

		if (tablename.equals("pras/getTable")) {
			paraMap.put("pagesize", pagesize);
		}else {
			paraMap.put("pagesize", pagesize);
			paraMap.put("starttime", starttime);
			paraMap.put("endtime", endtime);
		}

		String sign = BeaverUtils.getRequestSign(paraMap, appsecret);
//		System.out.println("sign = "+sign);
		return sign;
    }

    public static void main(String [] args) throws NoSuchAlgorithmException{
    	String appkey = "tmpAppKey";
    	String appsecret = "tmpAppSecret";
//    	String sign = GetResultServlet.createSign(appkey,appsecret);
//    	System.out.println(sign);
//    	sign = "c50f87030e7904ac497cc96194c0897e";
	}
    
}
