package com.gome.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class getRefChannel extends UDF {

    private String[] c1 = {"http://digital.gome.com.cn/",
            "http://computer.gome.com.cn/",
            "http://toysgifts.gome.com.cn/",
            "http://electronic.gome.com.cn/",
            "http://life.gome.com.cn/",
            "http://jiaju.gome.com.cn/",
            "http://decoration.gome.com.cn/",
            "http://health.gome.com.cn/",
            "http://luxury.gome.com.cn/",
            "http://furnit.gome.com.cn/",
            "http://book.gome.com.cn/",
            "http://alcohol.gome.com.cn/",
            "http://beauty.gome.com.cn/",
            "http://art.gome.com.cn/",
            "http://baby.gome.com.cn/",
            "http://watch.gome.com.cn/",
            "http://clothing.gome.com.cn/",
            "http://auto.gome.com.cn/",
            "http://jiafang.gome.com.cn/",
            "http://tongxun.gome.com.cn/"};

    public String evaluate(String refer_url) {
        String channel = "";

        try {
            if (refer_url.trim().length() == 0) {
                channel = "direct";
                return channel;
            }

            if (getDomain(refer_url.trim()).indexOf("gome.com.cn") < 0) {
                channel = "external";
                return channel;
            }

            if (refer_url.trim().startsWith("http://www.gome.com.cn/category/")) {
                channel = "c3";
                return channel;
            }

            if (refer_url.trim().startsWith("http://www.gome.com.cn/search")) {
                channel = "search";
                return channel;
            }

            if (refer_url.trim().startsWith("http://prom.gome.com.cn") || refer_url.trim().startsWith("http://www.gome.com.cn/html/prodhtml/topics")) {
                channel = "prom";
                return channel;
            }

            if (refer_url.trim().startsWith("http://www.gome.com.cn/pinpai/")) {
                channel = "brand";
                return channel;
            }

            if (refer_url.trim().startsWith("http://www.gome.com.cn/topic/")) {
                channel = "topic";
                return channel;
            }

            if (refer_url.trim().startsWith("http://www.gome.com.cn/keywords/")) {
                channel = "keywords";
                return channel;
            }


            if (refer_url.trim().equals("http://www.gome.com.cn/") || refer_url.trim().startsWith("http://www.gome.com.cn/?")) {
                channel = "homepage";
                return channel;
            }

            if (refer_url.trim().startsWith("http://www.gome.com.cn/product/A")) {
                channel = "product_ly";
                return channel;
            }

            if (refer_url.trim().startsWith("http://www.gome.com.cn/product/")) {
                channel = "product_zy";
                return channel;
            }

            if (!getDomain(refer_url.trim()).equals("www.gome.com.cn") && !getDomain(refer_url.trim()).equals("gome.com.cn")) {
                channel = "c1";
                return channel;
            }

            channel = "other";
        } catch (Exception ex) {
            channel = "error";
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
}














