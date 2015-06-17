package com.gome.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class getProductIdForUrl extends UDF {

//	public String evaluate(String page_url) {
//		StringBuffer pid = new StringBuffer();
//		String[] strs = page_url.replace("http://", "").split("/");
//		if (strs.length > 2 && strs[1].toLowerCase().equals("product")) {
//			int flag = 0;
//			for (int i = 0; i < strs[2].length(); i++) {
//				if (Character.isDigit(strs[2].charAt(i))) {
//					flag += 1;
//				} else if (flag > 0) {
//					break;
//				}
//				pid.append(strs[2].charAt(i));
//			}
//		}
//		return pid.toString();
//	}


    public String evaluate(String page_url) {
        String[] strs;

        if (page_url.startsWith("http://item.gome.com.cn/")) {
            strs = page_url.replace("http://", "").split("/");
            if (strs.length > 1) {
                return getPid(strs[1]);
            }
        }
        if (page_url.startsWith("http://www.gome.com.cn/product/")) {
            strs = page_url.replace("http://", "").split("/");
            if (strs.length > 2) {
                return getPid(strs[2]);
            }
        }

        return "";
    }

    private static String getPid(String url) {
        StringBuffer pid = new StringBuffer();

        int flag = 0;
        for (int i = 0; i < url.length(); i++) {
            if (Character.isDigit(url.charAt(i))) {
                flag += 1;
            } else if (flag > 0) {
                break;
            }
            pid.append(url.charAt(i));
        }

        return pid.toString();
    }
}
