/*
Navicat MySQL Data Transfer

Source Server         : shopassis2
Source Server Version : 50615
Source Host           : 10.126.50.115:8306
Source Database       : bigdata

Target Server Type    : MYSQL
Target Server Version : 50615
File Encoding         : 65001

Date: 2015-10-13 14:42:10
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for shop_offline_statistics
-- ----------------------------
DROP TABLE IF EXISTS `shop_offline_statistics`;
CREATE TABLE `shop_offline_statistics` (
  `ID` bigint(255) NOT NULL AUTO_INCREMENT,
  `SHOP_ID` varchar(36) DEFAULT NULL,
  `PAGEVIEWS` int(15) DEFAULT NULL,
  `USERVIEWS` int(15) DEFAULT NULL,
  `VISITS` int(15) DEFAULT NULL,
  `ORDERS` int(15) DEFAULT NULL,
  `RESIDENCE_TIME` int(50) DEFAULT NULL,
  `NEW_USERVIEWS` int(15) DEFAULT NULL,
  `JUMP_LOSS` int(15) DEFAULT NULL,
  `FDATE` varchar(36) DEFAULT NULL,
  `DATA_SOURCE` int(1) DEFAULT NULL COMMENT '1 web 2 wab ',
  `UPLOADTIME` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`ID`),
  KEY `SHOP_ID` (`SHOP_ID`),
  KEY `DATA_SOURCE` (`DATA_SOURCE`),
  KEY `FDATE` (`FDATE`)
) ENGINE=InnoDB AUTO_INCREMENT=2444880 DEFAULT CHARSET=utf8;
