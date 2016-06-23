package com.cloudbeaver;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import net.sf.json.JSONArray;

import org.junit.Assert;
import org.junit.Test;

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
	}

	@Test
	public void testString(){
		String dirConf = "/tmp/testpics?";
		String sub = dirConf.substring(dirConf.length() - 1);
		Assert.assertEquals(sub, "?");
	}
}
