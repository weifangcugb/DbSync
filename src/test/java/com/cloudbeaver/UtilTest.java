package com.cloudbeaver;

import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.junit.Assert;
import org.junit.Test;

import com.auth0.jwt.Algorithm;
import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.client.common.SqlHelper;
import com.cloudbeaver.client.dbbean.DatabaseBean;
import com.cloudbeaver.jwt.JWTSigner;

public class UtilTest {
	@Test
	public void testJsonArrayEmpty(){
		JSONArray jArray = new JSONArray();
		Assert.assertEquals(jArray.toString(),"[]");
	}

	@Test
	public void testDate() throws ParseException{
//		String dirConf = "/tmp/testpics?2016-06-22-12-00-00";
//		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-hh-mm-ss");
//		String time = dirConf.substring(dirConf.indexOf('?') + 1);

//		2016-07-15 16:55:01.0
//		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
//		Date date = sdf.parse("2016-07-15 16:55:01.0");
//		System.out.println(date);
//		System.out.println(date.getTime());

//		String time = "2016-07-15 16:55:01.0";
//		String result = time.substring(0, time.indexOf('.')).replaceAll("[-: ]", "");
//		System.out.println(result);

//		20160715165501
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		Date date = sdf.parse("20000101000000");
		System.out.println(date);
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.set(Calendar.SECOND, calendar.get(calendar.SECOND) + 1000);
		Date enDate = calendar.getTime();
		System.out.println(enDate);
		System.out.println(sdf.format(enDate));
	}

	@Test
	public void testTimeStamp() throws ParseException{
		long now = System.currentTimeMillis();
		String todayString = BeaverUtils.timestampToDateString(now);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		Date today = sdf.parse(todayString + " 00:00:00");
		System.out.println("timestamp:" + now + " today:" + todayString + " todayTimeStamp:" + today.getTime());
		System.out.println(now - now%(24 * 3600 * 1000));
		System.out.println(BeaverUtils.timestampToDateString(now - now%(24 * 3600 * 1000)));
		System.out.println(BeaverUtils.timestampToDateString(today.getTime() - 3 * 24 * 3600 * 1000));
		System.out.println(BeaverUtils.timestampToDateString(48 * 3600 * 1000));

		System.out.println(new Date(1467302400000l));
		System.out.println(new Date(1467331200000l));
		System.out.println(new Date(0l));
		Date date = new Date();
	}

	@Test
	public void testString(){
		String dirConf = "/tmp/testpics?";
		String sub = dirConf.substring(dirConf.length() - 1);
		Assert.assertEquals(sub, "?");
	}

	@Test
	public void testMD5() throws NoSuchAlgorithmException{
		Map<String, String> paraMap = new HashMap<String, String>();
		paraMap.put("appkey", "tmpKey");
		paraMap.put("pageno", "1");
		paraMap.put("pagesize", "20");
		paraMap.put("starttime", "2016-01-01");
		String sign = BeaverUtils.getRequestSign(paraMap, "tmpSecret");
		Assert.assertEquals(sign, "51C2D1697EB2293CA7E27E38ED99F813");
	}

	@Test
	public void testJson() {
		JSONObject jObject = new JSONObject();
		jObject.put("k2", "v2");
		jObject.element("k2", "v4");
//		jObject.put("k2", "v3");
		jObject.accumulate("k1", "v1");
		jObject.accumulate("k1", "v4");
		jObject.put("k3", "v3");
		jObject.put("k1", "v7");
		jObject.accumulate("k1", "v8");
		jObject.put("k5", "v5");
		System.out.println("object:" + jObject);
	}

	@Test
	public void testAppSign() throws NoSuchAlgorithmException{
//		test YouDi system sign
	    String appPreDefKey = "20150603";
	    String appPreDefSecret = "7454739E907F5595AE61D84B8547F574";

	    Map<String, String> paraMap = new HashMap<String, String>();
		paraMap.put("appkey", appPreDefKey);
		String sign = BeaverUtils.getRequestSign(paraMap, appPreDefSecret);
		Assert.assertEquals(sign, "D5421D0BCF81CA97810541D91897075A");
	}

	private void testSync() {
		Thread thread = new Thread(new Runnable() {
			public void run() {
//				sync.syncNow();
				sync.sayHello();
			}
		});
		thread.start();

//		BeaverUtils.sleep(1000);
//		sync.sayHello();
		sync.sayHi();
	}

	public static void main(String[] args) throws NoSuchAlgorithmException, ParseException {
		UtilTest uTest = new UtilTest();
//		uTest.testAppSign();
//		uTest.testBCrypt();
//		uTest.testSync();
//		uTest.testJwt();
		uTest.testDate();
	}

	private void testJwt() {

	}
}

class sync{
	public static synchronized void syncNow(){
		while(true){
			System.out.println("sleeping");
			BeaverUtils.sleep(1000);
		}
	}

	public static void sayHello(){
		while(true){
			System.out.println("hello");
			BeaverUtils.sleep(1000);
		}
	}

	public static void sayHi(){
		synchronized(sync.class){
			while(true){
				System.out.println("hi");
				BeaverUtils.sleep(1000);
			}
		}
	}
}
