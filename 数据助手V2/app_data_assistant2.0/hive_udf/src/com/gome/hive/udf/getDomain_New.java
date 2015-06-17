package com.gome.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by liushijie on 2014/11/10.
 */
public class getDomain_New extends UDF {
    public String evaluate(String url) {
        try {
            Pattern p = Pattern.compile("(?<=//|)((\\w)+\\.)+\\w+");
            Matcher m = p.matcher(url);
            if (m.find()) {
                return m.group();
            } else {
                return url;
            }
        }catch (Exception ex)
        {
            return url;
        }
    }

}
