package com.gome.hive.udf_new;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by liushijie on 2015/4/7.
 */
public class getCenterIDForUrl extends UDF {

    public String evaluate(String url) {
//        fsxb	服饰鞋包中心	运动户外	http://toysgifts.gome.com.cn/	cat10000002
        if (url.startsWith("http://toysgifts.gome.com.cn"))
            return "fsxb";
//        fsxb	服饰鞋包中心	钟表首饰	http://watch.gome.com.cn/	cat18596267
        if (url.startsWith("http://watch.gome.com.cn"))
            return "fsxb";
//        fsxb	服饰鞋包中心	箱包奢品	http://luxury.gome.com.cn/	cat10000083
        if (url.startsWith("http://luxury.gome.com.cn"))
            return "fsxb";
//        fsxb	服饰鞋包中心	服饰鞋帽	http://clothing.gome.com.cn/	cat18000002
        if (url.startsWith("http://clothing.gome.com.cn"))
            return "fsxb";
//        fsxb	服饰鞋包中心	文化艺术	http://art.gome.com.cn/	catx10000424
        if (url.startsWith("http://art.gome.com.cn"))
            return "fsxb";
//        fsxb	服饰鞋包中心	黄金收藏	http://gold.gome.com.cn/	cat18596267
        if (url.startsWith("http://gold.gome.com.cn"))
            return "fsxb";
//        jdzx	家电中心	手机	http://tongxun.gome.com.cn/	cat31665542
        if (url.startsWith("http://tongxun.gome.com.cn"))
            return "jdzx";
//        jdzx	家电中心	电视 冰箱 洗衣机 空调	http://electronic.gome.com.cn/	cat10000004
        if (url.startsWith("http://electronic.gome.com.cn"))
            return "jdzx";
//        jdzx	家电中心	生活电器 厨卫电器	http://life.gome.com.cn/	cat10000005
        if (url.startsWith("http://life.gome.com.cn"))
            return "jdzx";
//        jdzx	家电中心	电脑 办公打印 文仪	http://computer.gome.com.cn/	cat18000009
        if (url.startsWith("http://computer.gome.com.cn"))
            return "jdzx";
//        jdzx	家电中心	数码	http://digital.gome.com.cn/	cat18000008
        if (url.startsWith("http://digital.gome.com.cn"))
            return "jdzx";
//        jjjz	家居家装中心	家居日用	http://jiaju.gome.com.cn/	cat10000006
        if (url.startsWith("http://jiaju.gome.com.cn"))
            return "jjjz";
//        jjjz	家居家装中心	家装建材	http://decoration.gome.com.cn/	cat10000018
        if (url.startsWith("http://decoration.gome.com.cn"))
            return "jjjz";
//        jjjz	家居家装中心	家纺寝居	http://jiafang.gome.com.cn/	cat21426052
        if (url.startsWith("http://jiafang.gome.com.cn"))
            return "jjjz";
//        jjjz	家居家装中心	家具	http://furnit.gome.com.cn/	cat10000333
        if (url.startsWith("http://furnit.gome.com.cn"))
            return "jjjz";
//        kx	快销中心	母婴用品 玩具	http://baby.gome.com.cn/	cat15985715
        if (url.startsWith("http://baby.gome.com.cn"))
            return "kx";
//        kx	快销中心	美妆个护	http://beauty.gome.com.cn/	cat15555607
        if (url.startsWith("http://beauty.gome.com.cn"))
            return "kx";
//        kx	快销中心	健康医疗	http://health.gome.com.cn/	cat10000074
        if (url.startsWith("http://health.gome.com.cn"))
            return "kx";
//        kx	快销中心	食品酒水	http://alcohol.gome.com.cn/	cat15045969
        if (url.startsWith("http://alcohol.gome.com.cn"))
            return "kx";
//        qc	汽车中心	汽车用品	http://auto.gome.com.cn/	cat18596269
        if (url.startsWith("http://auto.gome.com.cn"))
            return "qc";
//        qc	汽车中心	汽车维修保养	http://auto.gome.com.cn/	cat32897448
        if (url.startsWith("http://auto.gome.com.cn"))
            return "qc";

//        lyjr	旅游金融中心	金融	http://jr.gome.com.cn/	无
        if (url.startsWith("http://jr.gome.com.cn"))
            return "lyjr";
//        lyjr	旅游金融中心	票据	http://piaoju.gome.com.cn/	无
        if (url.startsWith("http://piaoju.gome.com.cn"))
            return "lyjr";
//        lyjr	旅游金融中心	机票	http://jipiao.gome.com.cn/	无
        if (url.startsWith("http://jipiao.gome.com.cn"))
            return "lyjr";
//        qg	抢购业务部	限时抢购	http://q.gome.com.cn	无
        if (url.startsWith("http://q.gome.com.cn"))
            return "tq";
//        tg	团购业务部	团购	http://tuan.gome.com.cn	无
        if (url.startsWith("http://tuan.gome.com.cn"))
            return "tq";
//        ylsh	娱乐生活中心	充值	http://chongzhi.gome.com.cn/	无
        if (url.startsWith("http://chongzhi.gome.com.cn"))
            return "ylsh";
//        ylsh	娱乐生活中心	彩票	http://caipiao.gome.com.cn/	无
        if (url.startsWith("http://caipiao.gome.com.cn"))
            return "ylsh";
//        ylsh	娱乐生活中心	游戏 http://game.gome.com.cn/
        if (url.startsWith("http://game.gome.com.cn"))
            return "ylsh";

        return "";
    }

}
