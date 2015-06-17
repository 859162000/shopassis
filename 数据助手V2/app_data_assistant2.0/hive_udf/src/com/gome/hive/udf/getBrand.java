package com.gome.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by liushijie on 2014/12/16.
 */
public class getBrand extends UDF {
    public String evaluate(String page_url) {
        StringBuffer sb = new StringBuffer();
        String[] strs = page_url.replace("http://", "").split("/");
        if (strs.length > 2 && strs[1].toLowerCase().equals("pinpai")) {
            sb.append(strs[2]);
        }
        return sb.toString();
    }
}
