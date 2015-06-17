package com.gome.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.ArrayList;

/**
 * Created by liushijie on 2014/12/15.
 */
public class getTags extends UDF {

    final static Pattern P = Pattern.compile("(?<=//|)((\\w)+\\.)+\\w+");

    public ArrayList<String> evaluate(String page_url, String refer_url, String cm) {
        ArrayList<String> al = new ArrayList<String>();

        String page_type = getPageType(page_url);
        String refer_type = getReferType(refer_url);

        al = getReferTag(cm);
        String refer_tag = al.get(0);
        String tracking_code = al.get(1);
        String[] page_path = getPagePath(page_url);

        al.clear();
        al.add(page_type);
        al.add(refer_type);
        al.add(refer_tag);
        al.add(tracking_code);
        al.add(page_path[0]);
        al.add(page_path[1]);
        al.add(page_path[2]);
        al.add(page_path[3]);
        al.add(page_path[4]);

        return al;
    }

    public ArrayList<String> evaluate(String page_url, String refer_url) {
        ArrayList<String> al = new ArrayList<String>();

        String page_type = getPageType(page_url);
        String refer_type = getReferType(refer_url);

        al = getReferTag(page_url, refer_url);
        String refer_tag = al.get(0);
        String tracking_code = al.get(1);
        String[] page_path = getPagePath(page_url);

        al.clear();
        al.add(page_type);
        al.add(refer_type);
        al.add(refer_tag);
        al.add(tracking_code);
        al.add(page_path[0]);
        al.add(page_path[1]);
        al.add(page_path[2]);
        al.add(page_path[3]);
        al.add(page_path[4]);

        return al;
    }

    private String getPageType(String page_url) {
        String channel = "";

        try {
//            if (getDomain(page_url.trim()).indexOf("gome.com.cn") < 0) {
//                channel = "external";
//                return channel;
//            }

            if (page_url.trim().startsWith("http://www.gome.com.cn/category/")) {
                channel = "c3";
                return channel;
            }

            if (page_url.trim().startsWith("http://www.gome.com.cn/search")) {
                channel = "search";
                return channel;
            }

            if (page_url.trim().startsWith("http://prom.gome.com.cn") || page_url.trim().startsWith("http://www.gome.com.cn/html/prodhtml/topics")) {
                channel = "prom";
                return channel;
            }

            if (page_url.trim().startsWith("http://www.gome.com.cn/pinpai/")) {
                channel = "brand";
                return channel;
            }

            if (page_url.trim().startsWith("http://www.gome.com.cn/topic/")) {
                channel = "topic";
                return channel;
            }

            if (page_url.trim().startsWith("http://www.gome.com.cn/keywords/")) {
                channel = "keywords";
                return channel;
            }


            if (page_url.trim().equals("http://www.gome.com.cn/") || page_url.trim().startsWith("http://www.gome.com.cn/?")) {
                channel = "homepage";
                return channel;
            }

            if (page_url.trim().startsWith("http://www.gome.com.cn/product/A")) {
                channel = "product_ly";
                return channel;
            }

            if (page_url.trim().startsWith("http://www.gome.com.cn/product/")) {
                channel = "product_zy";
                return channel;
            }

            if (!getDomain(page_url.trim()).equals("www.gome.com.cn") && !getDomain(page_url.trim()).equals("gome.com.cn")) {
                channel = "c1";
                return channel;
            }

            channel = "other";
        } catch (Exception ex) {
            channel = "error";
        }
        return channel;
    }

    private String getReferType(String refer_url) {
        if (getDomain(refer_url).indexOf("gome.com.cn") > -1 || refer_url.indexOf("coo8.com") > -1) {
            return "internal";
        }
        return "external";
    }

    private ArrayList<String> getReferTag(String cm) {
        ArrayList<String> al = new ArrayList<String>();

        try {
            String tcs;
            if (cm.split("\\^!").length > 1) {
                tcs = cm.split("\\^!")[cm.split("\\^!").length - 1];
            } else {
                tcs = cm.split("\\^")[cm.split("\\^").length - 1];
            }

            String[] tokens = tcs.split("_");

            if (tokens[0].equals("dh")) {
                al.add("bd");
            } else if (tokens[0].equals("member")) {
                al.add("edm");
            } else if (tcs.matches("ppc_sem.*|ppc_baidu_pinpai.*|ppc_juejin.*|ppc_pinpaibd.*|ppc_pinpaisg.*|ppc_sogou.*|ppc_baidu.*|ppc_google.*|ppc_sogou_pinpai.*|ppc_pinpaiyd.*|ppc_pinpaiss.*|ppc_pinpai360.*")) {
                al.add("sem");
            } else if (tcs.matches("ppc_sogouhd.*|ppc_sinawebo.*|ppc_qqweibo.*")) {
                al.add("snm");
            } else if (tokens[0].equals("pro-")) {
                al.add("pro");
            } else if (tokens[0].equals("yj") && tokens.length > 1) {
                if (tokens[1].matches(".*50du.*|.*heima8.*|.*zhigou\\.com.*|.*zhimei.*|.*chanet.*|.*weiyi\\.com.*|.*manmanbuy\\.com.*|.*duomai\\.com.*|.*yiqifa.*|.*huihui.*")) {
                    al.add("cps");
                } else if (tokens[1].matches("weibo.com")) {
                    al.add("snm");
                }
            } else {
                al.add(tokens[0]);
            }


            al.add(cm);
        } catch (Exception ex) {
            al.add("error");
            al.add(cm);
        }

        return al;
    }

    private ArrayList<String> getReferTag(String page_url, String refer_url) {
        ArrayList<String> al = new ArrayList<String>();

        try {
            if (refer_url != null)
                //if(ref_url.indexOf("gome.com.cn") > -1 || ref_url.indexOf("coo8.com") > -1) {
                if (getDomain(refer_url).indexOf("gome.com.cn") > -1 || getDomain(refer_url).indexOf("coo8.com") > -1) {
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

            if (refer_url == null || refer_url.trim().isEmpty()) {
                al.add("direct");
                al.add("");
                return al;
            }

            String ref_domain = getDomain(refer_url);
            if (ref_domain.indexOf("so.com") > -1 || ref_domain.indexOf("baidu.com") > -1 || ref_domain.indexOf("sogou.com") > -1 || ref_domain.indexOf("google.com") > -1) {
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

    private String[] getPagePath(String page_url) {
        String[] al = {"", "", "", "", ""};

        String[] qs = page_url.replace("http://", "").split("/");
        if (qs.length > 1) {
            for (int i = 1; i < qs.length; i++) {
                if (i > 5) {
                    al[4] = al[4] + "," + qs[i];
                } else {
                    al[i - 1] = qs[i];
                }
            }
        }
        return al;
    }


    private String getDomain(String url) {

        Matcher m = P.matcher(url);
        if (m.find()) {
            return m.group();
        } else {
            return url;
        }
    }
}
