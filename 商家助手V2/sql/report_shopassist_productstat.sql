﻿/*
Navicat MySQL Data Transfer

Source Server         : shopassist
Source Server Version : 50166
Source Host           : 10.58.47.155:3306
Source Database       : data_helper

Target Server Type    : MYSQL
Target Server Version : 50166
File Encoding         : 65001

Date: 2015-04-21 17:23:21
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for report_shopassist_productstat
-- ----------------------------
DROP TABLE IF EXISTS `report_shopassist_productstat`;
CREATE TABLE `report_shopassist_productstat` (
  `shop_id` int(15) DEFAULT NULL COMMENT '店铺ID',
  `visit_date` int(15) DEFAULT NULL COMMENT '访问时间 YYYYMMDD',
  `data_source` smallint(1) DEFAULT NULL COMMENT '数据来源 1 web 2 wap 3 app',
  `product_id` varchar(36) DEFAULT NULL COMMENT '产品ID',
  `sku_id` varchar(36) DEFAULT NULL COMMENT 'SKUID  如果查产品维度，此字段为空',
  `uv` int(15) DEFAULT NULL COMMENT '独特访问数',
  `pv` int(15) DEFAULT NULL COMMENT '浏览量',
  `jumps` int(15) DEFAULT NULL COMMENT '总跳出次数',
  `remain_times` int(50) DEFAULT NULL COMMENT '总停留时间',
  `id` int(15) NOT NULL AUTO_INCREMENT COMMENT '自增，无意义，防止堆表',
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT='按产品/SKU统计';
