package com.cloudbeaver;

import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.junit.Assert;
import org.junit.Test;
//import org.mindrot.jbcrypt.BCrypt;
import org.mindrot.jbcrypt.BCrypt;

import com.cloudbeaver.client.common.BeaverUtils;

public class UtilTest {
	@Test
	public void testJsonArrayEmpty(){
		JSONArray jArray = new JSONArray();
		Assert.assertEquals(jArray.toString(),"[]");
	}

	@Test
	public void testDate() throws ParseException{
		String dirConf = "/tmp/testpics?2016-06-22-12-00-00";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-hh-mm-ss");
		String time = dirConf.substring(dirConf.indexOf('?') + 1);
		Date date = sdf.parse(time);
		System.out.println(date);
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

	@Test
	public void testBCrypt(){
		// Hash a password for the first time
//		String hashed = BCrypt.hashpw("apple", BCrypt.gensalt());
		String hashed = BCrypt.hashpw("apple", "$2a$10$E4QgvqjJQjOv8bM19vvkJu");
//		$2a$10$E4QgvqjJQjOv8bM19vvkJuvE.1fTdhoMF8TuLUIBobBISXesmF9MO
		System.out.println(hashed);
		// gensalt's log_rounds parameter determines the complexity
		// the work factor is 2**log_rounds, and the default is 10
//		String hashed = BCrypt.hashpw(password, BCrypt.gensalt(12));

		// Check that an unencrypted password matches one that has
		// previously been hashed
		if (BCrypt.checkpw("apple", hashed))
			System.out.println("It matches");
		else
			System.out.println("It does not match");
	}

	public static void main(String[] args) throws NoSuchAlgorithmException {
		UtilTest uTest = new UtilTest();
//		uTest.testAppSign();
		uTest.testBCrypt();
	}
}
