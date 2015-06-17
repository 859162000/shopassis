package com.gome.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by liushijie on 2015/1/13.
 */
public class getURLS extends UDF {

    final static Pattern P = Pattern.compile("(?<=//|)((\\w)+\\.)+\\w+");
    ArrayList<String> al = new ArrayList<String>();

    public ArrayList<String> evaluate(String url) {
        al.clear();

        if (url == null || url.isEmpty()) {
            al.add("");
            al.add("");
        }
        al.add(getDomain(url));
        al.add(getUrl(url));

        return al;
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
