package com.gome.hive.udf;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDF;

public class getChannelForCampaign extends UDF {

	public String evaluate(String campaign, String refer_domain,
			String refer_url, String order_id) {
		String channel = "";

		try {

			if (!order_id.trim().isEmpty()) {
				return "order";
			}

			if (!campaign.isEmpty()) {
				channel = campaign.replace("-", "_").split("_")[0];
			}

			if (channel.isEmpty()) {

				if (refer_domain.trim().isEmpty()) {
					refer_domain = this.getDomain(refer_url);
				}

				if (refer_domain.trim().equals("so.com")) {
					channel = "seo-360";
				} else if (refer_domain.trim().equals("baidu.com")) {
					channel = "seo-baidu";
				} else if (refer_domain.trim().equals("sogou.com")) {
					channel = "seo-sogou";
				} else if (refer_domain.trim().length() > 0
						&& !refer_domain.trim().equals("gome.com.cn")) {
					channel = "seo-other";
				} else if (refer_domain.trim().equals("gome.com.cn")) {
					channel = "internal";
				} else if (refer_domain.trim().length() == 0) {
					channel = "direct";
				}
			}

		} catch (Exception ex) {
			return "error";
		}

		return channel;
	}

	private String getDomain(String url) {
		Pattern p = Pattern.compile("(?<=//|)((\\w)+\\.)+\\w+");
		Matcher m = p.matcher(url);
		if (m.find()) {
			return m.group();
		} else {
			return url;
		}
	}

}
