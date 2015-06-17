package com.gome.hive.udf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDF;

public class gome_mapping extends UDF {
	public ArrayList<String> evaluate(String channel, String logtime,
			String page_url, String ref_url, String ref_domain) {

		String[] channels = (channel.split(",").length != 5 ? format(channel, 5)
				.split(",")
				: channel.split(","));
		String[] logtimes = (logtime.split(",").length != 5 ? format(logtime, 5)
				.split(",")
				: logtime.split(","));
		String[] page_urls = (page_url.split(",").length != 5 ? format(
				page_url, 5).split(",") : page_url.split(","));
		String[] ref_urls = (ref_url.split(",").length != 5 ? format(ref_url, 5)
				.split(",")
				: ref_url.split(","));
		String[] ref_domains = (ref_domain.split(",").length != 5 ? format(
				ref_domain, 5).split(",") : ref_domain.split(","));

		String[] result = new String[5];
		ArrayList<String> al = new ArrayList<String>();

		for (int i = 0; i < 5; i++) {
			result[i] = channels[i] + "," + logtimes[i] + "," + page_urls[i]
					+ "," + ref_urls[i] + "," + ref_domains[i];
			al.add(channels[i] + "," + logtimes[i] + "," + page_urls[i] + ","
					+ ref_urls[i] + "," + ref_domains[i]);
		}

		return al;
	}

	private String format(String a, int length) {
		String[] b = a.split(",");
		StringBuffer sb = new StringBuffer();

		for (int i = 0; i < length; i++) {
			if (i >= b.length)
				sb.append("{null},");
			else {
				System.out.println(i + ":" + (b[i].isEmpty() ? "nulll" : b[i])
						+ ":" + b.length);
				sb.append((b[i].isEmpty() ? "{null}" : b[i]) + ",");
			}
		}

		return sb.toString();
	}
}
