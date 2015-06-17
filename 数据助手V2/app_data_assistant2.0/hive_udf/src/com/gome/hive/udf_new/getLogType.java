package com.gome.hive.udf_new;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.Comparator;
import java.util.TreeMap;

/**
 * Created by liushijie on 2015/4/27.
 */
public class getLogType extends UDF {

    public String evaluate(String lpg, Long record_timestamp) {
        String[] lpgs;
        Long temp_timestamp = 0L;
        Long lpg_timestamp = 0L;
        String result = "null";

        try {
            lpgs = lpg.split(",");
            for (String s : lpgs) {
                lpg_timestamp = Long.parseLong(s.split("-")[1]);
                if (lpg_timestamp > temp_timestamp && lpg_timestamp <= record_timestamp) {
                    temp_timestamp = lpg_timestamp;
                    result = s.split("-")[0];
                }
            }
        } catch (Exception ex) {
            result = "error";
        }
        return result;
    }
}

