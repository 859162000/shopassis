package com.gome.hive.udf;

import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDF;

public class getPageUrlTag extends UDF {
	public String evaluate(String page_url) {

		String list = ".*gome.com.cn/category/cat.+";
		String list_click = ".*gome.com.cn/category/cat.+-.*";

		String product = ".*gome.com.cn/product/.+";

		String search = ".*gome.com.cn/search\\?question=.+";
		String search_click = ".*gome.com.cn/search\\?question=.+&.+";

		if (Pattern.matches(product, page_url)) {
			return "product";
		}

		if (Pattern.matches(list_click, page_url)) {
			return "list_click";
		} else if (Pattern.matches(list, page_url)) {
			return "list";
		}

		if (Pattern.matches(search_click, page_url)) {
			return "search_click";
		} else if (Pattern.matches(search, page_url)) {
			return "search";
		}

		return page_url;
	}
}