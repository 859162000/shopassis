package com.gome.hive.udf_new;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;

/**
 * Created by liushijie on 2015/3/24.
 */
public class getChannelTag extends UDF {

    ArrayList<String> al = new ArrayList<String>();
    String tcs = null;

    public ArrayList<String> evaluate(String cm) {
        al.clear();


        try {

            if (cm.split("\\^!").length > 1) {
                tcs = cm.split("\\^!")[cm.split("\\^!").length - 1];
            } else {
                tcs = cm.split("\\^")[cm.split("\\^").length - 1];
            }

            String[] tokens = tcs.split("_");

            if (tokens[0].equals("seo")) {
                al.add(tokens[0]);//channel
                al.add(tokens[1]);//se
                if (tokens[2].matches("国美商城|国美在线商城|guomei|国美电器|国美电器官网|国美电器网上商城|国美在线官网|国美商城官网|国美网上商城|国美网站|gome|www.gome.com.cn |国美电器城|国国美官网|国美|国美在线|库巴|库巴网")) {
                    al.add("brand");//is brand
                } else {
                    al.add("nobrand");//is brand
                }
                al.add(tokens[2]);//word
            } else if (tokens[0].equals("sem")) {
                al.add(tokens[0]);//channel
                al.add(tokens[1]);//se
                if (tokens[2].equals("pinpai")) {
                    //pinzhuan
                    al.add("pinzhuan");
                    al.add(tcs);//pinzhuan
                } else {
                    //word
                    al.add("word");
                    if (tokens[3].startsWith("品牌")) {
                        al.add("pinpai");
                    } else if (tokens[3].startsWith("竞品")) {
                        al.add("jingpin");
                    } else if (tokens[3].startsWith("通用")) {
                        al.add("tongyong");
                    } else {
                        al.add("chanping");
                    }
                    al.add(tokens[3]);//word
                }
            } else if (tcs.matches("ppc_sem.*|ppc_baidu_pinpai.*|ppc_juejin.*|ppc_pinpaibd.*|ppc_pinpaisg.*|ppc_sogou.*|ppc_baidu.*|ppc_google.*|ppc_sogou_pinpai.*|ppc_pinpaiyd.*|ppc_pinpaiss.*|ppc_pinpai360.*")) {
                al.add("sem");
                al.add("old");
                al.add("old");
                al.add("old");
                al.add("old");
                al.add("old");
            } else if (tokens[0].equals("ad")) {
                al.add(tokens[0]);
                if (tcs.matches("ad_pconline.*|ad_it168.*|ad_pcpop.*|ad_yesky.*|ad_wanwei.*|ad_zol.*")) {
                    al.add("chuizhi");
                    al.add(tokens[1]);
                } else if (tcs.matches("ad_163.*|ad_ifeng.*|ad_sohu.*|ad_sogou_tips.*")) {
                    al.add("menhu");
                    if (tokens[1].equals("sogou")) {
                        al.add("sogou_tips");
                    } else {
                        al.add(tokens[1]);
                    }
                } else if (tcs.matches("ad_aiqiyi.*|ad_qq_dakanban.*|ad_qq_zanting.*|ad_youku.*|ad_splm.*|ad_bdplayer.*|ad_mangguo.*")) {
                    al.add("shipin");
                    if (tokens[1].equals("qq")) {
                        if (tokens[2].equals("dakanban")) {
                            al.add("qq_dakanban");
                        } else {
                            al.add("qq_zanting");
                        }
                    } else {
                        al.add(tokens[1]);
                    }
                } else if (tcs.matches("ad_sogou_moon.*|ad_youjian.*|ad_guangdiantong.*")) {
                    al.add("jingzhun");
                    if (tokens[1].equals("sogou")) {
                        al.add("sogou_moon");
                    } else {
                        al.add(tokens[1]);
                    }
                } else {
                    al.add("other");
                    al.add(tokens[1]);
                }
            } else if (tokens[0].equals("bd")) {
                al.add(tokens[0]);
                al.add(tokens[1]);
            } else if (tokens[0].equals("pro")) {
                al.add(tokens[0]);
                al.add(tokens[1]);
            } else if (tokens[0].equals("yj")) {
                al.add("yj");
                al.add(tokens[1]);
            } else if (tokens[0].equals("dsp")) {
                al.add(tokens[0]);
                al.add(tokens[1]);
            } else if (tokens[0].equals("snm")) {
                al.add(tokens[0]);
                if (tokens[1].matches("weibo|weixin|bbs")) {
                    al.add(tokens[1]);
                } else {
                    al.add("other");
                }
            } else if (tcs.matches("member_email.*|member_duanxin.*")) {
                al.add("edm");
                al.add(tokens[1]);
                if (tokens[2].equals("xwl") || tokens[2].equals("ddl")) {
                    al.add("chufa");
                } else {
                    al.add("shoudong");
                }
            } else if (tokens[0].equals("cps")) {
                al.add(tokens[0]);
                al.add(tokens[1]);
            } else {
                al.add(tokens[0]);
                al.add(tcs);
            }
        } catch (Exception ex) {
            al.add("error");
            al.add("error");
            al.add("error");
            al.add(tcs);
        }

        return al;
    }
}
