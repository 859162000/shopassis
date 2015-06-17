package com.gome.hive.udf;

import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDF;

public class getURLType extends UDF {

	/*
	 * input URL
	 * 
	 * list http://www.gome.com.cn/category/cat10000235.html
	 * 
	 * list_click
	 * http://www.gome.com.cn/category/cat10000235-00-0-48-1-0-0-0-1-12
	 * yp-0-0-0-0-0-0-0-0.html
	 * 
	 * product http://www.gome.com.cn/product/9131380533.html
	 * 
	 * search http://www.gome.com.cn/search?question=%E7%94%B5%E5%86%B0%E7%AE%B1
	 * 
	 * search_click
	 * http://www.gome.com.cn/search?question=%E7%94%B5%E5%86%B0%E7%
	 * AE%B1&deliv=0&pageSize=48&facet=102t&pzpq=0&pzin=v5
	 * 
	 * 
	 * regexp_replace(regexp_extract(page_url,'(cat\\d+).*?.html',1),'cat',''),
	 * where log_date >=2014080100 and log_date <=2014080123 and page_url like
	 * '%category/cat%' limit 100;"
	 */
	public String evaluate(String URL) {

		// String list="http://www.gome.com.cn/category/cat10000235.html";
		// String
		// list_click="http://www.gome.com.cn/category/cat10000235-00-0-48-1-0-0-0-1-12yp-0-0-0-0-0-0-0-0.html";
		// String product="http://www.gome.com.cn/product/9131380533.html";
		// String
		// search="http://www.gome.com.cn/search?question=%E7%94%B5%E5%86%B0%E7%AE%B1";
		// String
		// search_click="http://www.gome.com.cn/search?question=%E7%94%B5%E5%86%B0%E7%AE%B1&deliv=0&pageSize=48&facet=102t&pzpq=0&pzin=v5";
		//		
		// System.out.println(Pattern.matches(".*gome.com.cn/category/cat.+",list));
		// System.out.println(Pattern.matches(".*gome.com.cn/category/cat\\d+-.*",list_click));
		//		
		// System.out.println(Pattern.matches(".*gome.com.cn/product/\\d+.+",product));
		//		
		// System.out.println(Pattern.matches(".*gome.com.cn/search\\?question=.+",search));
		// System.out.println(Pattern.matches(".*gome.com.cn/search\\?question=.+&.+",search_click));

		String list = ".*gome.com.cn/category/cat.+";
		String list_click = ".*gome.com.cn/category/cat\\d+-.*";
		String product = ".*gome.com.cn/product/.+";
		String search = ".*gome.com.cn/search\\?question=.+";
		String search_click = ".*gome.com.cn/search\\?question=.+&.+";

		String tag = "other";

		if (Pattern.matches(list_click, URL)) {
			tag = "list_click";
			return tag;
		} else if (Pattern.matches(list, URL)) {
			tag = "list";
			return tag;
		}

		if (Pattern.matches(search_click, URL)) {
			tag = "search_click";
			return tag;
		} else if (Pattern.matches(search, URL)) {
			tag = "search";
			return tag;
		}

		if (Pattern.matches(product, URL)) {
			tag = "product";
			return tag;
		}

		return tag;
	}
	
}


















