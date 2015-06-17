package com.gome.hive.udf;

import java.sql.Date;
import java.text.SimpleDateFormat;

import org.apache.hadoop.hive.ql.exec.UDF;

public class gome_from_unixtime extends UDF {
	public String evaluate(String time) {
		StringBuffer sb = new StringBuffer();

		String[] times = time.split(",");

		int cnt = 0;

		for (String s : times) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String dates = sdf.format(new Date(Long.parseLong(s) * 1000L));

			if (cnt == 0)
				sb.append(dates);
			else
				sb.append("," + dates);
			cnt += 1;
		}

		return sb.toString();
	}

}
