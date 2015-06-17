package com.gome.hive.udtf;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class ExplodeMapOrders_New extends GenericUDTF {

    @Override
    public void close() throws HiveException {
        // TODO Auto-generated method stub
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args)
            throws UDFArgumentException {
        if (args.length != 8) {
            throw new UDFArgumentLengthException(
                    "ExplodeMapOrders takes only one argument");
        }
        if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException(
                    "ExplodeMapOrders takes string as a parameter");
        }

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("order_time");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        fieldNames.add("order_id");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        fieldNames.add("order_money");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        fieldNames.add("uid");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        fieldNames.add("channel");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        fieldNames.add("log_time");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        fieldNames.add("visitor_id");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        fieldNames.add("tracking_code");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        fieldNames.add("direct");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        fieldNames.add("indirect");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        fieldNames.add("days");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(
                fieldNames, fieldOIs);
    }

    @Override
     public void process(Object[] args) throws HiveException {
        String order_time = args[0].toString();
        String order_id = args[1].toString();
        String order_money = args[2].toString();
        String uid = args[3].toString();

        String channel = args[4] == null ? "" : args[4].toString();
        String log_time = args[5] == null ? "" : args[5].toString();
        String visitor_id = args[6] == null ? "" : args[6].toString();
        String  tracking_code = args[7] == null ? "" : args[7].toString();



        String[] channels = channel.split(",");
        String[] log_times = log_time.split(",");
        String[] visitor_ids = visitor_id.split(",");
        String[] tracking_codes = tracking_code.split(",");

        String results[] = new String[11];

        HashMap<String, Long> haschannel = new HashMap<String, Long>();

        long diffDays = 0;
        long diff = 0;

        try {
            if(log_time.trim().isEmpty() || log_time.trim().length() < 10)
            {
                results[0] = order_time;
                results[1] = order_id;
                results[2] = order_money;
                results[3] = uid;
                results[4] = "-1";
                results[5] = "-1";
                results[6] = "-1";
                results[7] = "-1";
                results[8] = "-1";
                results[9] = "-1";
                results[10] = "-1";

                forward(results);
                return;
            }



            for (int i = 0; i < channels.length; i++) {
                if (channels[i].equals("direct") && haschannel.size() == 0
                        && i < channels.length - 1) {
                    continue;
                }

                diffDays = dateDiff( log_times[i],order_time);
                diff = dateDiffSec(log_times[i],order_time);

                if (diff < 0 ) {
                    if (i == channels.length - 1 && haschannel.size() == 0) {
                        results[0] = order_time;
                        results[1] = order_id;
                        results[2] = order_money;
                        results[3] = uid;
                        results[4] = "-1";
                        results[5] = "-1";
                        results[6] = "-1";
                        results[7] = "-1";
                        results[8] = "-1";
                        results[9] = "-1";
                        results[10] = "-1";

                        forward(results);
                        return;
                    }
                    else
                    {
                        continue;
                    }
                }

                if (haschannel.size() == 0) {
                    results[0] = order_time;
                    results[1] = order_id;
                    results[2] = order_money;
                    results[3] = uid;
                    results[4] = channels[i];
                    results[5] = log_times[i];
                    results[6] = visitor_ids[i];
                    results[7] = i < tracking_codes.length ? tracking_codes[i] : "";
                    results[8] = "1";
                    results[9] = "0";
                    results[10] = Long.toString(diffDays);

                    haschannel.put(channels[i], diffDays);
                    forward(results);
                } else if (!haschannel.containsKey(channels[i])){
                    results[0] = order_time;
                    results[1] = order_id;
                    results[2] = order_money;
                    results[3] = uid;
                    results[4] = channels[i];
                    results[5] = log_times[i];
                    results[6] = visitor_ids[i];
                    results[7] = i < tracking_codes.length ? tracking_codes[i] : "";
                    results[8] = "0";
                    results[9] = "1";
                    results[10] = Long.toString(diffDays);

                    haschannel.put(channels[i], diffDays);
                    forward(results);
                } else if (haschannel.get(channels[i]) != diffDays) {
                    results[0] = order_time;
                    results[1] = order_id;
                    results[2] = order_money;
                    results[3] = uid;
                    results[4] = channels[i];
                    results[5] = log_times[i];
                    results[6] = visitor_ids[i];
                    results[7] = i < tracking_codes.length ? tracking_codes[i] : "";
                    results[8] = "0";
                    results[9] = "1";
                    results[10] = Long.toString(diffDays);

                    haschannel.put(channels[i], diffDays);
                    forward(results);
                }
            }
        } catch (Exception e) {
            results[0] = order_time;
            results[1] = order_id;
            results[2] = order_money;
            results[3] = uid;
            results[4] = channel;
            results[5] = log_time;
            results[6] = visitor_id;
            results[7] = tracking_code;
            results[8] = "-2";
            results[9] = "-2";
            results[10] = "-2";

            forward(results);
        }

    }

    private long dateDiff(String startTime, String endTime)
            throws java.text.ParseException {
        // 按照传入的格式生成一个simpledateformate对象
        SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd");
        long nd = 1000 * 24 * 60 * 60;// 一天的毫秒数
        // long nh = 1000 * 60 * 60;// 一小时的毫秒数
        // long nm = 1000 * 60;// 一分钟的毫秒数
        // long ns = 1000;// 一秒钟的毫秒数
        long diff;

        // 获得两个时间的毫秒时间差异
        diff = sd.parse(endTime).getTime() - sd.parse(startTime).getTime();
        long day = diff / nd;// 计算差多少天
        // long hour = diff % nd / nh;// 计算差多少小时
        // long min = diff % nd % nh / nm;// 计算差多少分钟
        // long sec = diff % nd % nh % nm / ns;// 计算差多少秒
        // 输出结果
        return day + 1;

    }

    private long dateDiffSec(String startTime, String endTime)
            throws java.text.ParseException {
        // 按照传入的格式生成一个simpledateformate对象
        SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd");
        long nd = 1000 * 24 * 60 * 60;// 一天的毫秒数
         long nh = 1000 * 60 * 60;// 一小时的毫秒数
         long nm = 1000 * 60;// 一分钟的毫秒数
         long ns = 1000;// 一秒钟的毫秒数
        long diff;

        // 获得两个时间的毫秒时间差异
        diff = sd.parse(endTime).getTime() - sd.parse(startTime).getTime();
        //long day = diff / nd;// 计算差多少天
        // long hour = diff % nd / nh;// 计算差多少小时
        // long min = diff % nd % nh / nm;// 计算差多少分钟
         long sec = diff % nd % nh % nm / ns;// 计算差多少秒
        // 输出结果
        return diff;

    }


}


