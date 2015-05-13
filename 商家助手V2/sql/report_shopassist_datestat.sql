/*
Navicat MySQL Data Transfer

Source Server         : shopassist
Source Server Version : 50166
Source Host           : 10.58.47.155:3306
Source Database       : data_helper

Target Server Type    : MYSQL
Target Server Version : 50166
File Encoding         : 65001

Date: 2015-04-21 17:22:46
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for report_shopassist_datestat
-- ----------------------------
DROP TABLE IF EXISTS `report_shopassist_datestat`;
CREATE TABLE `report_shopassist_datestat` (
  `shop_id` int(15) DEFAULT NULL COMMENT '店铺ID',
  `visit_date` int(15) DEFAULT NULL COMMENT '访问日期 YYYYMMDD',
  `visit_hour` int(15) DEFAULT NULL COMMENT '访问小时 YYYYMMDDHH (如果是天维度，此字段为-1)',
  `data_source` smallint(1) DEFAULT NULL COMMENT '数据来源  1 web 2 wap 3 app',
  `uv` int(15) DEFAULT NULL COMMENT '独特访客数',
  `new_uv` int(15) DEFAULT NULL COMMENT '新访客数',
  `pv` int(15) DEFAULT NULL COMMENT '浏览量',
  `visits` int(15) DEFAULT NULL COMMENT '访问数',
  `jumps` int(15) DEFAULT NULL COMMENT '总跳出次数',
  `remain_times` int(50) DEFAULT NULL COMMENT '总停留时间',
  `order_uv` int(15) DEFAULT NULL COMMENT '下单买家数',
  `id` int(15) NOT NULL AUTO_INCREMENT COMMENT '自增，无意义，防止堆表',
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT='按时间统计';
