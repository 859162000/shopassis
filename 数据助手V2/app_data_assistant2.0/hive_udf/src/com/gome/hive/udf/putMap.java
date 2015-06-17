package com.gome.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.Iterator;

public class putMap extends UDF {

    private MapWritable result = new MapWritable();

    public MapWritable evaluate(MapWritable map, Text key, Text value) {
        result.clear();

        Iterator<Writable> iter = map.keySet().iterator();
        Writable k;
        Writable v;

        while (iter.hasNext()) {
            k = iter.next();
            v = map.get(k);
            result.put(k, v);
        }
        result.put(key, value);

        return result;
    }
}
