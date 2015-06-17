package com.gome.hive.udf_new;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by liushijie on 2015/3/23.
 */
public class getUrlTag extends UDF {

    ArrayList<String> al = new ArrayList<String>();
    final static Pattern P = Pattern.compile("(?<=//|)((\\w)+\\.)+\\w+");

    public ArrayList<String> evaluate(String url) {
        al.clear();

        if (url == null || url.isEmpty()) {
            al.add("empty");
            al.add("empty");
            al.add("empty");
            al.add("empty");
            al.add("empty");
        }

        al.add(getDomain(url));
        al.add(getUrl(url));
        al.add(getType(url));
        if (getDomain(url).indexOf("gome.com.cn") < 0) {
            al.add("external");
        } else {
            al.add("internal");
        }
        al.add("empty");
        return al;
    }

    public ArrayList<String> evaluate(String url, Long timestamp) {
        al.clear();






        
        if (url == null || url.isEmpty()) {
            al.add("empty");
            al.add("empty");
            al.add("empty");
            al.add("empty");
            al.add("empty");
        }

        al.add(getDomain(url));
        al.add(getUrl(url));
        al.add(getType(url));
        if (getDomain(url).indexOf("gome.com.cn") < 0) {
            al.add("external");
        } else {
            al.add("internal");
        }
        al.add(timestamp.toString());

        return al;
    }

    private String getType(String url) {
//        促销页面(prom)：http://prom.gome.com.cn/html/prodhtml/****
//        搜索页面(seach)：http://www.gome.com.cn/search?question=****
//        首页：http://www.gome.com.cn/
//        超市页面：
//                    http://toysgifts.gome.com.cn/
//                    http://watch.gome.com.cn/
//                    http://luxury.gome.com.cn/
//                    http://clothing.gome.com.cn/
//                    http://art.gome.com.cn/
//                    http://gold.gome.com.cn/
//                    http://tongxun.gome.com.cn/
//                    http://electronic.gome.com.cn/
//                    http://life.gome.com.cn/
//                    http://computer.gome.com.cn/
//                    http://digital.gome.com.cn/
//                    http://jiaju.gome.com.cn/
//                    http://decoration.gome.com.cn/
//                    http://jiafang.gome.com.cn/
//                    http://furnit.gome.com.cn/
//                    http://baby.gome.com.cn/
//                    http://beauty.gome.com.cn/
//                    http://health.gome.com.cn/
//                    http://alcohol.gome.com.cn/
//                    http://auto.gome.com.cn/
//        列表页面：http://www.gome.com.cn/category/***
//        店铺页面：http://mall.gome.com.cn/****
//        详情页面：http://www.gome.com.cn/product/****.html
//        购物车页：http://g.gome.com.cn/ec/homeus/cart/cart.jsp
//        填写订单：http://g.gome.com.cn/ec/homeus/checkout/shipping.jsp
//        订单成功：http://g.gome.com.cn/ec/homeus/checkout/commitSuccess.jsp
        if (url.startsWith("http://prom.gome.com.cn/html/prodhtml/")) {
            return "prom";
        } else if (url.startsWith("http://www.gome.com.cn/search?question=") || url.startsWith("http://search.gome.com.cn")) {
            return "search";
        } else if (getUrl(url).equals("http://www.gome.com.cn/")) {
            return "home";
        } else if (getUrl(url).equals("http://toysgifts.gome.com.cn/") ||
                getUrl(url).equals("http://watch.gome.com.cn/") ||
                getUrl(url).equals("http://luxury.gome.com.cn/") ||
                getUrl(url).equals("http://clothing.gome.com.cn/") ||
                getUrl(url).equals("http://art.gome.com.cn/") ||
                getUrl(url).equals("http://gold.gome.com.cn/") ||
                getUrl(url).equals("http://tongxun.gome.com.cn/") ||
                getUrl(url).equals("http://electronic.gome.com.cn/") ||
                getUrl(url).equals("http://life.gome.com.cn/") ||
                getUrl(url).equals("http://computer.gome.com.cn/") ||
                getUrl(url).equals("http://digital.gome.com.cn/") ||
                getUrl(url).equals("http://jiaju.gome.com.cn/") ||
                getUrl(url).equals("http://decoration.gome.com.cn/") ||
                getUrl(url).equals("http://jiafang.gome.com.cn/") ||
                getUrl(url).equals("http://furnit.gome.com.cn/") ||
                getUrl(url).equals("http://baby.gome.com.cn/") ||
                getUrl(url).equals("http://beauty.gome.com.cn/") ||
                getUrl(url).equals("http://health.gome.com.cn/") ||
                getUrl(url).equals("http://alcohol.gome.com.cn/") ||
                getUrl(url).equals("http://auto.gome.com.cn/")) {
            return "supermarket";
        } else if (url.startsWith("http://www.gome.com.cn/category/") || url.startsWith("http://list.gome.com.cn")) {
            return "list";
        } else if (url.startsWith("http://mall.gome.com.cn/")) {
            return "shop";
        } else if (url.startsWith("http://www.gome.com.cn/product/") || url.startsWith("http://item.gome.com.cn")) {
            return "product";
        } else if (url.startsWith("http://g.gome.com.cn/ec/homeus/cart/cart.jsp")) {
            return "shopcart";
        } else if (url.startsWith("http://g.gome.com.cn/ec/homeus/checkout/shipping.jsp")) {
            return "fillorder";
        } else if (url.startsWith("http://g.gome.com.cn/ec/homeus/checkout/commitSuccess.jsp")) {
            return "successorder";
        } else if (url.startsWith("http://brand.gome.com.cn")) {
            return "brand";
        } else {
            return "other";
        }


    }

    private String getUrl(String url) {
        if (url == null || url.isEmpty())
            return "";

        if (url.indexOf("?") > -1)
            return url.substring(0, url.indexOf("?"));
        else
            return url;
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

