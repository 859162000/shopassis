/*
Navicat MySQL Data Transfer

Source Server         : shopassist
Source Server Version : 50166
Source Host           : 10.58.47.155:3306
Source Database       : data_helper

Target Server Type    : MYSQL
Target Server Version : 50166
File Encoding         : 65001

Date: 2015-04-21 17:23:51
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for report_shopassist_sourcepagestat
-- ----------------------------
DROP TABLE IF EXISTS `report_shopassist_sourcepagestat`;
CREATE TABLE `report_shopassist_sourcepagestat` (
  `shop_id` int(15) DEFAULT NULL COMMENT '店铺ID',
  `visit_date` int(15) DEFAULT NULL COMMENT '访问时间 YYYYMMDD',
  `visit_hour` int(15) DEFAULT NULL COMMENT 'YYYYMMDDHH，如果是天维度，此字段为-1',
  `data_source` smallint(1) DEFAULT NULL COMMENT '数据来源 1 web 2 wap 3 app',
  `source_ptid` smallint(5) DEFAULT NULL COMMENT '来源页面类型',
  `source_page` varchar(5000) DEFAULT NULL COMMENT '来源页面url',
  `uv` int(15) DEFAULT NULL COMMENT '落地页类型',
  `landing_pv` int(15) DEFAULT NULL COMMENT '落地页浏览量',
  `pv` int(15) DEFAULT NULL COMMENT '浏览量',
  `productinfo_pv` int(15) DEFAULT NULL COMMENT '商品详情页浏览量',
  `cart_pv` int(15) DEFAULT NULL COMMENT '购物车浏览量',
  `order_pv` int(15) DEFAULT NULL COMMENT '订单量',
  `pay_pv` int(15) DEFAULT NULL COMMENT '付款量',
  `visits` int(15) DEFAULT NULL COMMENT '访问数',
  `jumps` int(15) DEFAULT NULL COMMENT '总跳出次数',
  `remain_times` int(50) DEFAULT NULL COMMENT '总停留时间',
  `id` int(15) NOT NULL AUTO_INCREMENT COMMENT '主键',
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT='按来源页面统计';


create index idx_shopdate on report_shopassist_sourcepagestat(shop_id, data_source, visit_date,source_ptid); 
