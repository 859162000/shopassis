package com.gome.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;

public class formatAction extends UDF {

    public String evaluate(String action) {
        String[] pre_actions = action.trim().split(",");
        StringBuffer sb = new StringBuffer();

        ArrayList<String> al = new ArrayList<String>();

        for(int i = 0 ; i< pre_actions.length ; i++ )
        {
            if (pre_actions[i].indexOf("^DeLiMtTeD^") > 0)
            {
                al.add(pre_actions[i]);
            }
        }

        String[] actions = new String[al.size()];
        al.toArray(actions);

        sb.append(actions[0].split("\\^DeLiMtTeD\\^")[1]);

        if(actions.length == 1)
        {
            return sb.toString();
        }



        for (int i = 1; i < actions.length; i++) {
            if (!actions[i].split("\\^DeLiMtTeD\\^")[1].equals(actions[i - 1].split("\\^DeLiMtTeD\\^")[1]))
                sb.append("," + actions[i].split("\\^DeLiMtTeD\\^")[1]);
        }
        return sb.toString();
    }
}
