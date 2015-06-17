package com.gome.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class countDelimiter extends UDF {

    public int evaluate(String qd,String deli) {
        return qd.split(deli).length;
    }
}
