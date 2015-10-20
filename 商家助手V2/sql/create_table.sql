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
  is_new_user int,
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
 is_new_user int,
 log_hour string)
partitioned by (type string);
alter table shopdata_base_shopInfo_2 add partition (type = 'inhalf');
alter table shopdata_base_shopInfo_2 add partition (type = 'inall');
alter table shopdata_base_shopInfo_2 add partition (type = 'outhalf');
alter table shopdata_base_shopInfo_2 add partition (type = 'outall');

drop table shopdata_base_shopinfojump_2;
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
  is_new_user int,
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

drop table shopdata_shopstat_result;
create table shopdata_shopstat_result(
 id string,
 shopId string,
 pv bigint,
 uv bigint,
 visits bigint,
 orders bigint,
 remains bigint,
 new_users bigint,
 jumps bigint,
 visit_date string,
 i string,
 uploadtime string);

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

drop table shopdata_base_session_first;
create table shopdata_base_session_first(
 i int,
 shopId string,
 uc string,
 s string,
 ptId int,
 ru string,
 stId string,
 rownum int);
 
drop table shopdata_session_cartorder_product;
create table shopdata_session_cartorder_product(
 session_id string,
 site_id int,
 custom_action_id int,
 productId string);
 
drop table shopdata_session_cartorder_shop;
create table shopdata_session_cartorder_shop(
 session_id string,
 site_id int,
 custom_action_id int,
 shopId string);
 
drop table shopdata_session_cartorder_pv;
create table shopdata_session_cartorder_pv(
 session_id string,
 site_id int,
 shopId string,
 cart_pv bigint,
 order_pv bigint);
 
drop table shopdata_session_withoutpay;
create table shopdata_session_withoutpay(
 i int,
 shopId string,
 s string,
 orgin_time string,
 pv bigint,
 jumps bigint,
 remains bigint,
 productinfo_pv bigint,
 cart_pv bigint,
 order_pv bigint);

drop table report_productshop;
CREATE TABLE report_productshop(
  product_id string, 
  shop_no string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

load data local inpath '/home/data01/data/pagetypeid.txt' into table pagetype;

load data local inpath '/home/hadoop/shopassistant/shopid.txt' into table report_productshop;


drop table shop_real_time;
CREATE  TABLE shop_real_time
(
id string, 
SHOP_ID string,
PAGEVIEWS int,
USERVIEWS int,
FDATE_DAY string,
FDATE_HOUR string,
DATA_SOURCE int,
UPLOADTIME string
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' ;  

drop table shop_offline_visit_statistics;
CREATE  TABLE shop_offline_visit_statistics
( 
 id string,
 SHOP_ID string,                 
 VISIT_ID   string,              
 COOKIE_ID    string,            
 INTO_SHOP_TIME    string,       
 INTO_SHOP_TYPE    string,       
 INTO_SHOP_PAGE    string,       
 LANDING_PAGE      string,       
 SINGLE_VISIT_PAGE_NUMBER int,
 SINGLE_STAY_TIME         int,
 IS_NEW_USERVIEWS         int,
 USER_ID                  string,
 USER_POST_PROVINCE       string,
 USER_POST_CITY           string,
 FDATE                    string,
 DATA_SOURCE              int,
 UPLOADTIME               string
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' ;    
  
   create table shop_stat_res_tmp (SHOP_ID string,VISIT_ID string,COOKIE_ID string,SINGLE_VISIT_PAGE_NUMBER int ,SINGLE_STAY_TIME int) row format delimited fields terminated by '\t'  stored as textfile;
  create table shop_res_tmp (SHOP_ID string,VISIT_ID string,COOKIE_ID string,INTO_SHOP_TIME string,INTO_SHOP_TYPE string,INTO_SHOP_PAGE string,LANDING_PAGE string,IS_NEW_USERVIEWS int ,user_id string,USER_POST_PROVINCE string,
  USER_POST_CITY string,fdate string,data_source string,uploadtime string,rownumber int) row format delimited fields terminated by '\t'  stored as textfile;
   create table shop_res_tmp_2 (SHOP_ID string,VISIT_ID string,COOKIE_ID string,INTO_SHOP_TIME string,INTO_SHOP_TYPE string,INTO_SHOP_PAGE string,LANDING_PAGE string,IS_NEW_USERVIEWS int ,user_id string,USER_POST_PROVINCE string,
  USER_POST_CITY string,fdate string,data_source string,uploadtime string,rownumber int) row format delimited fields terminated by '\t'  stored as textfile;
   create table shop_res_tmp_3 (SHOP_ID string,VISIT_ID string,COOKIE_ID string,INTO_SHOP_TIME string,INTO_SHOP_TYPE string,INTO_SHOP_PAGE string,LANDING_PAGE string,IS_NEW_USERVIEWS int ,user_id string,USER_POST_PROVINCE string,
  USER_POST_CITY string,fdate string,data_source string,uploadtime string,rownumber int) row format delimited fields terminated by '\t'  stored as textfile;
  
  
  
  
  CREATE  TABLE shopdata_base_real_time(
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
  log_hour string,
  u string,
  city_id string,
  province_id string,
  is_new_user int,
  log_day string)
PARTITIONED BY (
  type string)
row format delimited fields terminated by '\t';

CREATE  TABLE shopdata_base_shopInfo_2_real_time(
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
  log_hour string, 
  u string, 
  city_id string, 
  province_id string, 
  is_new_user int, 
  log_day string)
PARTITIONED BY ( 
  type string)
  row format delimited fields terminated by '\t';

create table pagetype
(
 Id Int,
 Name string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

drop table shop_realtime;
CREATE  TABLE shop_realtime
(
id string, 
SHOP_ID string,
PAGEVIEWS int,
USERVIEWS int,
FDATE_DAY string,
FDATE_HOUR string,
DATA_SOURCE int,
UPLOADTIME string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
