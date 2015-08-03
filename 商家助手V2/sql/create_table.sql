drop table shopdata_base;
CREATE TABLE shopdata_base(
  uc string, 
  i int, 
  s string, 
  ptid int, 
  ru string, 
  stid string, 
  rustid string,
  shopid string, 
  productid string, 
  skuid string, 
  rshopid string, 
  rproductid string, 
  new_time string, 
  remains bigint,
  log_hour string)
partitioned by (type string);
alter table shopdata_base add partition (type = 'in');
alter table shopdata_base add partition (type = 'out');

drop table shopdata_base_shopInfo_2;
CREATE TABLE shopdata_base_shopInfo_2(
 uc string, 
 ptid int, 
 ru string, 
 stid string,
 rustid string,
 i int, 
 s string, 
 shopid string, 
 productid string, 
 skuid string, 
 rshopid string, 
 rproductid string, 
 new_time string, 
 remains bigint,
 log_hour string)
partitioned by (type string);
alter table shopdata_base_shopInfo_2 add partition (type = 'inhalf');
alter table shopdata_base_shopInfo_2 add partition (type = 'inall');
alter table shopdata_base_shopInfo_2 add partition (type = 'outhalf');
alter table shopdata_base_shopInfo_2 add partition (type = 'outall');

CREATE TABLE shopdata_base_shopinfojump_2(
  i int, 
  uc string, 
  ptid int, 
  ru string, 
  stid string, 
  rustid string, 
  s string, 
  shopid string, 
  productid string, 
  skuid string, 
  new_time string, 
  remains bigint, 
  log_hour string, 
  is_jump bigint);

drop table shopdata_productsku_result;
create table shopdata_productsku_result(
 shopId string,
 visit_date string,
 i string,
 productId string,
 skuId string,
 uv bigint,
 pv bigint,
 jumps bigint,
 remains bigint,
 id string)
partitioned by (type string);
alter table shopdata_productsku_result add partition (type = 'product');
alter table shopdata_productsku_result add partition (type = 'sku');

drop table shopdata_shoppagestat_result;
create table shopdata_shoppagestat_result(
 shopId string,
 visit_date string,
 visit_hour string,
 i string,
 stId string,
 uv bigint,
 pv bigint,
 jumps bigint,
 remains bigint,
 visits bigint, 
 id string)
partitioned by (type string);
alter table shopdata_shoppagestat_result add partition (type = 'day');
alter table shopdata_shoppagestat_result add partition (type = 'hour');

drop table shopdata_aimpagestat_result;
CREATE TABLE shopdata_aimpagestat_result(
 shopId string, 
 visit_date string, 
 i string, 
 rustId string, 
 stId string, 
 tid string, 
 uv bigint, 
 pv bigint, 
 id string);

drop table shopdata_sesstion_3;
create table shopdata_sesstion_3(
 i string,
 shopId string,
 s string,
 orgin_time string,
 pv bigint,
 jumps bigint, 
 remains bigint, 
 productinfo_pv bigint);

drop table shopdata_sesstion_order_4; 
create table shopdata_sesstion_order_4(
 s string,
 cart_pv bigint,
 order_pv bigint, 
 pay_pv bigint);

drop table shopdata_uv_5;
create table shopdata_uv_5(
 i string,
 shopId string,
 ptId string,
 ru string,
 uv bigint);

drop table shopdata_sesstion_5;
 create table shopdata_sesstion_5(
 i string,
 shopid string,  
 s string,
 orgin_time string,
 pv bigint, 
 jumps bigint, 
 remains bigint, 
 productinfo_pv bigint, 
 cart_pv bigint, 
 order_pv bigint, 
 pay_pv bigint);

drop table shopdata_shopInfo_6; 
create table shopdata_shopInfo_6(
 i string,
 shopid string,  
 ptid string, 
 ru string, 
 landing_pv bigint, 
 pv bigint, 
 productinfo_pv bigint, 
 cart_pv bigint, 
 order_pv bigint, 
 pay_pv bigint, 
 visits bigint, 
 jumps bigint, 
 remains bigint, 
 id string);

drop table shopdata_result; 
CREATE  TABLE shopdata_result(
  shopid string, 
  visit_date string, 
  visit_hour int, 
  data_source string, 
  ptid string, 
  ru string, 
  uv bigint, 
  landing_pv bigint, 
  pv bigint, 
  productinfo_pv bigint, 
  cart_pv bigint, 
  order_pv bigint, 
  pay_pv bigint, 
  visits bigint, 
  jumps bigint, 
  remains bigint, 
  id string);

drop table shopdata_shopout1_a;
CREATE  TABLE shopdata_shopout1_a(
  uc string,
  i int,
  s string,
  ptid int,
  ru string,
  stid string,
  rustid string,
  shopid string,
  productid string,
  skuid string,
  rshopid string,
  rproductid string,
  new_time string);

drop table shopdata_shopout1_b;
CREATE  TABLE shopdata_shopout1_b(
  uc string,
  i int,
  s string,
  ptid int,
  ru string,
  stid string,
  rustid string,
  shopid string,
  productid string,
  skuid string,
  rshopid string,
  rproductid string,
  new_time string);

drop table shopdata_shopout2;
CREATE  TABLE shopdata_shopout2(
  uc string,
  i int,
  s string,
  ptid int,
  ru string,
  stid string,
  rustid string,
  shopid string,
  productid string,
  skuid string,
  rshopid string,
  rproductid string,
  new_time string);

drop table shopdata_shopout3;
CREATE  TABLE shopdata_shopout3(
  rshopid string,
  i int,
  rustid string,
  ptid int,
  uv int,
  pv int)
partitioned by (type string);
alter table shopdata_shopout3 add partition (type = 'in');
alter table shopdata_shopout3 add partition (type = 'out');