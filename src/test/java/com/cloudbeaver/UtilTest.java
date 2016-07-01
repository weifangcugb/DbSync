package com.cloudbeaver;

import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import net.sf.json.JSONArray;

import org.junit.Assert;
import org.junit.Test;

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
		Assert.assertEquals(sign, "d90d440655ab3d3dcf96c3487378756d");
	}
}
