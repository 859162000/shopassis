/*
Navicat MySQL Data Transfer

Source Server         : shopassis2
Source Server Version : 50615
Source Host           : 10.126.50.115:8306
Source Database       : bigdata

Target Server Type    : MYSQL
Target Server Version : 50615
File Encoding         : 65001

Date: 2015-10-13 14:42:21
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for shop_offline_visit_statistics
-- ----------------------------
DROP TABLE IF EXISTS `shop_offline_visit_statistics`;
CREATE TABLE `shop_offline_visit_statistics` (
  `ID` bigint(255) NOT NULL AUTO_INCREMENT,
  `SHOP_ID` varchar(36) DEFAULT NULL,
  `VISIT_ID` varchar(36) DEFAULT NULL,
  `COOKIE_ID` varchar(36) DEFAULT NULL,
  `INTO_SHOP_TIME` datetime DEFAULT NULL,
  `INTO_SHOP_TYPE` varchar(36) DEFAULT NULL,
  `INTO_SHOP_PAGE` varchar(5000) DEFAULT NULL,
  `LANDING_PAGE` varchar(5000) DEFAULT NULL,
  `SINGLE_VISIT_PAGE_NUMBER` int(15) DEFAULT NULL,
  `SINGLE_STAY_TIME` int(50) DEFAULT NULL,
  `IS_NEW_USERVIEWS` int(1) DEFAULT NULL COMMENT '1 新 2 老',
  `USER_ID` varchar(36) DEFAULT NULL,
  `USER_POST_PROVINCE` varchar(36) DEFAULT NULL,
  `USER_POST_CITY` varchar(36) DEFAULT NULL,
  `FDATE` varchar(36) DEFAULT NULL,
  `DATA_SOURCE` int(1) DEFAULT NULL,
  `UPLOADTIME` datetime DEFAULT NULL,
  PRIMARY KEY (`ID`),
  KEY `SHOP_ID` (`SHOP_ID`),
  KEY `COOKIE_ID` (`COOKIE_ID`),
  KEY `DATA_SOURCE` (`DATA_SOURCE`),
  KEY `FDATE` (`FDATE`)
) ENGINE=InnoDB AUTO_INCREMENT=101887879 DEFAULT CHARSET=utf8;
