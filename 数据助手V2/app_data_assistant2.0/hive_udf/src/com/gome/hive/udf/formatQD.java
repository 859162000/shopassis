package com.gome.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class formatQD extends UDF {

	public String evaluate(String qd) {

		String[] temp = qd.trim().split(",");
		StringBuffer sb = new StringBuffer();

        try {
            sb.append(temp[0]);

            for (int i = 1; i < temp.length; i++) {

                if (!temp[i].equals(temp[i - 1]))
                    sb.append("," + temp[i]);

            }
        }catch(Exception ex)
        {
        }
		return sb.toString();
	}
}
