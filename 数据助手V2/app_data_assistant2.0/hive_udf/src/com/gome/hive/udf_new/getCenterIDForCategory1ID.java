package com.gome.hive.udf_new;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by liushijie on 2015/4/7.
 */
public class getCenterIDForCategory1ID extends UDF {

    public String evaluate(String category1id) {
//        fsxb	服饰鞋包中心	运动户外	http://toysgifts.gome.com.cn/	cat10000002
//        fsxb	服饰鞋包中心	钟表首饰	http://watch.gome.com.cn/	cat18596267
//        fsxb	服饰鞋包中心	箱包奢品	http://luxury.gome.com.cn/	cat10000083
//        fsxb	服饰鞋包中心	服饰鞋帽	http://clothing.gome.com.cn/	cat18000002
//        fsxb	服饰鞋包中心	文化艺术	http://art.gome.com.cn/	catx10000424
//        fsxb	服饰鞋包中心	黄金收藏	http://gold.gome.com.cn/	cat18596267
        if (category1id.equals("cat10000002") || category1id.equals("cat18596267") || category1id.equals("cat10000083") || category1id.equals("cat18000002") || category1id.equals("catx10000424") || category1id.equals("cat18596267"))
            return "fsxb";

//        jdzx	家电中心	手机	http://tongxun.gome.com.cn/	cat31665542
//        jdzx	家电中心	电视 冰箱 洗衣机 空调	http://electronic.gome.com.cn/	cat10000004
//        jdzx	家电中心	生活电器 厨卫电器	http://life.gome.com.cn/	cat10000005
//        jdzx	家电中心	电脑 办公打印 文仪	http://computer.gome.com.cn/	cat18000009
//        jdzx	家电中心	数码	http://digital.gome.com.cn/	cat18000008
        if (category1id.equals("cat31665542") || category1id.equals("cat10000004") || category1id.equals("cat10000005") || category1id.equals("cat18000009") || category1id.equals("cat18000008"))
            return "jdzx";

//        jjjz	家居家装中心	家居日用	http://jiaju.gome.com.cn/	cat10000006
//        jjjz	家居家装中心	家装建材	http://decoration.gome.com.cn/	cat10000018
//        jjjz	家居家装中心	家纺寝居	http://jiafang.gome.com.cn/	cat21426052
//        jjjz	家居家装中心	家具	http://furnit.gome.com.cn/	cat10000333
        if (category1id.equals("cat10000006") || category1id.equals("cat10000018") || category1id.equals("cat21426052") || category1id.equals("cat10000333"))
            return "jjjz";

//        kx	快销中心	母婴用品 玩具	http://baby.gome.com.cn/	cat15985715
//        kx	快销中心	美妆个护	http://beauty.gome.com.cn/	cat15555607
//        kx	快销中心	健康医疗	http://health.gome.com.cn/	cat10000074
//        kx	快销中心	食品酒水	http://alcohol.gome.com.cn/	cat15045969
        if (category1id.equals("cat15985715") || category1id.equals("cat15555607") || category1id.equals("cat10000074") || category1id.equals("cat15045969"))
            return "kx";

//        qc	汽车中心	汽车用品	http://auto.gome.com.cn/	cat18596269
//        qc	汽车中心	汽车维修保养	http://auto.gome.com.cn/	cat32897448
        if (category1id.equals("cat18596269") || category1id.equals("cat32897448"))
            return "kx";

        return "";
    }

}
