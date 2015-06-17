package com.gome.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class getOrderConvTags extends UDF {
	public String evaluate(String t) {
		String input = t;

		String[] tags;

		if (input.indexOf("product") == 0)
			return "product";
		else if (input.indexOf("product") > 0)
			input = input.substring(0, input.indexOf("product") - 1);

		tags = input.split(",");

		if (tags.length == 1 && tags[0].equals("direct"))
			return "direct";

		for (int i = 0; i < tags.length; i++) {
			try {
				if (tags[i].equals("direct")) 
					continue;
				else
					return tags[i];
			} catch (Exception ex) {
				continue;
			}
		}
		return "null";
	}
}
