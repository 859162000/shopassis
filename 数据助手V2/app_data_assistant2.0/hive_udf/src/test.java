import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by liushijie on 2015/2/15.
 */
public class test {

    public static void main(String[] args) throws Exception {


        String str = "国美商城s";
        if (str.matches("国美商城|国美在线商城|guomei|国美电器|国美电器官网|国美电器网上商城|国美在线官网|国美商城官网|国美网上商城|国美网站|gome|www.gome.com.cn |国美电器城|国国美官网|国美|国美在线|库巴|库巴网")) {
            System.out.println("brand");//is brand
            System.out.println(str);//is brand
        } else {
            System.out.println("nobrand");//is brand
            System.out.println(str);//is brand
        }
        System.exit(1);

        SortedMap<Long, String> sm = new TreeMap<Long, String>(new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                return (int) (o2 - o1);
            }
        });

        sm.put(10L, "10");
        sm.put(101L, "10");
        sm.put(1L, "10");
        sm.put(11L, "10");

        for (Long l : sm.keySet()) {
            System.out.println(l);
        }
        System.exit(1);

        if ("gomes.com.cn".indexOf("gome.com.cn") < 0) {
            System.out.println("Ex");
        } else {
            System.out.println("In");
        }
        System.exit(0);


        String cm = "a^!b^c";
        String tcs;
        if (cm.split("\\^!").length > 1) {
            tcs = cm.split("\\^!")[cm.split("\\^!").length - 1];
        } else {
            tcs = cm.split("\\^")[cm.split("\\^").length - 1];
        }
        System.out.println(tcs);
        System.exit(0);
    }

    private static MapWritable result = new MapWritable();

    public static MapWritable evaluate(MapWritable map, Text key, Text value) {
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

    public static String evaluate(String url) {
        try {
            Pattern p = Pattern.compile("(?<=//|)((\\w)+\\.)+\\w+");
            Matcher m = p.matcher(url);
            if (m.find()) {
                return m.group();
            } else {
                return url;
            }
        } catch (Exception ex) {
            return url;
        }
    }
}

