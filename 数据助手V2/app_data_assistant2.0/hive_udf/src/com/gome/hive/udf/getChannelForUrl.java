package com.gome.hive.udf;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDF;


public class getChannelForUrl extends UDF {

	public String evaluate(String page_url, String refer_domain,
			String refer_url, String order_id) {
		String channel = "";

		try {

			if (!order_id.trim().isEmpty())
				return "order";

			String[] qs = page_url.trim().substring(page_url.indexOf("?") + 1)
					.split("&");

			for (String s : qs) {
				if (s.startsWith("cmpid")) {
					channel = s.replace("cmpid", "").replace("=", "").replace(
							"-", "_").trim().split("_")[0];
					break;
				}
			}

			if (channel.isEmpty()) {

				if (refer_domain.trim().isEmpty()) {
					refer_domain = this.getDomain(refer_url);
				}

				if (refer_domain.trim().equals("so.com"))
					channel = "seo-360";
				else if (refer_domain.trim().equals("baidu.com"))
					channel = "seo-baidu";
				else if (refer_domain.trim().equals("sogou.com"))
					channel = "seo-sogou";
				else if (refer_domain.trim().length() > 0
						&& !refer_domain.trim().equals("gome.com.cn"))
					channel = "seo-other";
				else if (refer_domain.trim().equals("gome.com.cn"))
					channel = "internal";
				else if (refer_domain.trim().length() == 0)
					channel = "direct";
			}

		} catch (Exception ex) {
			return "error";
		}

		return channel;
	}

	private String getDomain(String url) {
		Pattern p = Pattern.compile("(?<=//|)((\\w)+\\.)+\\w+");
		Matcher m = p.matcher(url);
		if (m.find()) {
			return m.group();
		} else {
			return url;
		}
	}
	
	
	
	
//	
//	
//	
//	public  String evaluate(String page_url,
//			String refer_domain, String refer_url, String product_id) {
//		String channel = "";
//		try {
//
//			if (!product_id.trim().isEmpty())
//				return "product";
//
//			String[] qs = page_url.trim().substring(page_url.indexOf("?") + 1)
//					.split("&");
//
//			for (String s : qs) {
//				if (s.startsWith("cmpid")) {
//					channel = s.replace("cmpid", "").replace("=", "").replace(
//							"-", "_").trim().split("_")[0];
//					break;
//				}
//			}
//
//			if (channel.isEmpty()) {
//
//
//				if (refer_url == null || refer_url.isEmpty()) {
//					return "direct";
//				}
//
//				if (refer_domain.trim().isEmpty()) {
//					refer_domain = getDomain(refer_url);
//				}
//
//				System.out.println("==========" + refer_domain);
//
//				if (refer_domain.trim().indexOf("so.com") > -1)
//					channel = "seo-360";
//				else if (refer_domain.trim().indexOf("baidu.com") > -1)
//					channel = "seo-baidu";
//				else if (refer_domain.trim().indexOf("sogou.com") > -1)
//					channel = "seo-sogou";
//				else if (refer_domain.trim().length() > 4
//						&& refer_domain.trim().indexOf("gome.com.cn") < 0) {
//					channel = "seo-other";
//				} else if (refer_domain.trim().length() <= 4)
//					channel = "direct";
//				else
//					channel = "internal";
//			}
//
//		} catch (Exception ex) {
//			return "error";
//		}
//
//		return channel;
//	}
//
//	private String getDomain(String url) {
//
//		try {
//			URL aURL = new URL(url);
//			return aURL.getHost();
//		} catch (MalformedURLException e) {
//			e.printStackTrace();
//		}
//		return "";
//
//	}
}
