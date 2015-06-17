package com.gome.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by liushijie on 2014/11/7.
 */

public class getChannel extends UDF {

    final static Pattern p = Pattern.compile("(?<=//|)((\\w)+\\.)+\\w+");

    public ArrayList<String> evaluate(String page_url, String ref_url) {

        ArrayList<String> al = new ArrayList<String>();

        try {
            if (ref_url != null)
                //if(ref_url.indexOf("gome.com.cn") > -1 || ref_url.indexOf("coo8.com") > -1) {
                if (getDomains(ref_url).indexOf("gome.com.cn") > -1 || getDomains(ref_url).indexOf("coo8.com") > -1) {
                    al.add("internal");
                    al.add("");
                    return al;
                }

            String channel = null;
            String tracking_code = null;
            String[] qs = page_url.trim().substring(page_url.trim().indexOf("?") + 1).split("&");

            for (String s : qs) {
                if (s.startsWith("cmpid")) {
                    tracking_code = s.split("=")[1];
                    channel = s.split("=")[1].replace("-", "_").split("_")[0];
                    al.add(channel);
                    al.add(tracking_code);
                    return al;
                }
            }

            if (ref_url == null || ref_url.trim().isEmpty()) {
                al.add("direct");
                al.add("");
                return al;
            }

            String ref_domain = getDomains(ref_url);
            if (ref_domain.equals("so.com") || ref_domain.equals("baidu.com") || ref_domain.equals("sogou.com") || ref_domain.equals("google.com")) {
                al.add("seo");
                al.add("");
            } else {
                al.add("referral");
                al.add("");
            }
            return al;
        } catch (Exception e) {
            al.add("error");
            al.add("error");
            return al;
        }
    }

    private String getDomains(String url) {
        Matcher m = p.matcher(url);
        if (m.find()) {
            return m.group();
        } else {
            return url;
        }
    }
}
