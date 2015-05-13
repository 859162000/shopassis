/*
Navicat MySQL Data Transfer

Source Server         : shopassist
Source Server Version : 50166
Source Host           : 10.58.47.155:3306
Source Database       : data_helper

Target Server Type    : MYSQL
Target Server Version : 50166
File Encoding         : 65001

Date: 2015-04-21 17:23:06
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for report_shopassist_pagetype
-- ----------------------------
DROP TABLE IF EXISTS `report_shopassist_pagetype`;
CREATE TABLE `report_shopassist_pagetype` (
  `pt_id` smallint(5) DEFAULT NULL COMMENT '页面类型ID',
  `pt_name` varchar(36) DEFAULT NULL COMMENT '页面类型名称  参见需求文档所有页面类型'
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT='页面类型字典表 ';
