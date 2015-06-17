package com.gome.hive.udf;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDF;

public class getCatIdForUrl extends UDF {
	public String evaluate(String page_url) {
		
//		String s1 = "http://www.gome.com.cn/category/cat10000203.html";
//		String s2 = "http://www.gome.com.cn/category/cat10000203.html?121313";
//
//		String s3 = "http://www.gome.com.cn/category/catx10000203.html";
//		String s4 = "http://www.gome.com.cn/category/catx10000203.html?121313";
//
//		String s5 = "http://www.gome.com.cn/search?question=http://www.gome.com.cn/category/cat21445795-00-0-36-0-0-0-0-1-0-0-1-0-0-0-0-0-0.html";

		String[] strs = page_url.replace("http://", "").split("/");
		if (strs.length > 2 && strs[1].toLowerCase().equals("category")
				&& strs[2].toLowerCase().startsWith("cat")) {
			
			Pattern p = Pattern.compile("((cat|catx)\\d+)");
			Matcher m = p.matcher(strs[2]);
			if (m.find()) {
				return m.group();
			}
		}

		return null;
	}
}
