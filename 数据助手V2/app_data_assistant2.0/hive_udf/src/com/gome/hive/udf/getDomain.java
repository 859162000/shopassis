package com.gome.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by liushijie on 2014/11/10.
 */
public class getDomain extends UDF {
    public String evaluate(String url) {
        try {
            Pattern p = Pattern.compile("(?<=http://|\\.)[^.]*?\\.(com.cn|com|cn|net|org|biz|info|cc|tv)", Pattern.CASE_INSENSITIVE);
            Matcher matcher = p.matcher(url);
            matcher.find();
            return matcher.group();
        }catch (Exception ex)
        {
            return url;
        }
    }

}
