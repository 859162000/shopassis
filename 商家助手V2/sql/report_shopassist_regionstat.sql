/*
Navicat MySQL Data Transfer

Source Server         : shopassist
Source Server Version : 50166
Source Host           : 10.58.47.155:3306
Source Database       : data_helper

Target Server Type    : MYSQL
Target Server Version : 50166
File Encoding         : 65001

Date: 2015-04-21 17:23:34
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for report_shopassist_regionstat
-- ----------------------------
DROP TABLE IF EXISTS `report_shopassist_regionstat`;
CREATE TABLE `report_shopassist_regionstat` (
  `shop_id` int(15) DEFAULT NULL COMMENT '店铺ID',
  `visit_date` int(15) DEFAULT NULL COMMENT '访问时间 YYYYMMDD',
  `data_source` smallint(1) DEFAULT NULL COMMENT '数据来源 1 web 2 wap 3 app',
  `province` int(15) DEFAULT NULL COMMENT '省',
  `uv` int(15) DEFAULT NULL COMMENT '独特访客数',
  `pv` int(15) DEFAULT NULL COMMENT '浏览量',
  `order_pv` int(15) DEFAULT NULL COMMENT '订单量',
  `order_uv` int(15) DEFAULT NULL COMMENT '下单买家数',
  `id` int(15) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT='按区域统计';
