/*
Navicat MySQL Data Transfer

Source Server         : shopassist
Source Server Version : 50166
Source Host           : 10.58.47.155:3306
Source Database       : data_helper

Target Server Type    : MYSQL
Target Server Version : 50166
File Encoding         : 65001

Date: 2015-05-13 22:16:23
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for report_shopassist_aimpagestat
-- ----------------------------
DROP TABLE IF EXISTS `report_shopassist_aimpagestat`;
CREATE TABLE `report_shopassist_aimpagestat` (
  `shop_id` int(15) DEFAULT NULL,
  `visit_date` int(15) DEFAULT NULL,
  `data_source` smallint(1) DEFAULT NULL,
  `shop_ptid` smallint(5) DEFAULT NULL,
  `aim_ptid` smallint(5) DEFAULT NULL,
  `tid` smallint(1) DEFAULT NULL COMMENT '目标类型   1 来源  2 去向',
  `uv` int(15) DEFAULT NULL,
  `pv` int(15) DEFAULT NULL,
  `id` int(15) NOT NULL AUTO_INCREMENT COMMENT '自增，无意义，防止堆表',
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT='按去向页面统计';
