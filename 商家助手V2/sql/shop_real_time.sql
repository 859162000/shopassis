/*
Navicat MySQL Data Transfer

Source Server         : shopassis2
Source Server Version : 50615
Source Host           : 10.126.50.115:8306
Source Database       : bigdata

Target Server Type    : MYSQL
Target Server Version : 50615
File Encoding         : 65001

Date: 2015-10-13 14:42:28
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for shop_real_time
-- ----------------------------
DROP TABLE IF EXISTS `shop_real_time`;
CREATE TABLE `shop_real_time` (
  `ID` bigint(255) NOT NULL AUTO_INCREMENT,
  `SHOP_ID` varchar(36) DEFAULT NULL,
  `PAGEVIEWS` int(15) DEFAULT NULL,
  `USERVIEWS` int(15) DEFAULT NULL,
  `FDATE_DAY` varchar(36) DEFAULT NULL,
  `FDATE_HOUR` varchar(36) DEFAULT NULL,
  `DATA_SOURCE` int(1) DEFAULT NULL,
  `UPLOADTIME` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`ID`),
  KEY `SHOP_ID` (`SHOP_ID`),
  KEY `DATA_SOURCE` (`DATA_SOURCE`),
  KEY `FDATE_DAY` (`FDATE_DAY`),
  KEY `FDATE_HOUR` (`FDATE_HOUR`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
