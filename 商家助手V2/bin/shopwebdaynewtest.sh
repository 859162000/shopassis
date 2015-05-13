#!/bin/bash
cd /home/hadoop/mikai

datelong=`date +%Y%m%d`

/home/hadoop/hive/bin/hive -e "
use test_shopasis;
add jar /home/hadoop/mikai/hiveudfall.jar;
create temporary function getshopinfouat as 'test.GetUatShopInfo';

drop table temp_shopwebdata_1;
create table temp_shopwebdata_1 as
select uc,new_time,s,product_platform,v1,ru,ip,u,uid,log_day,log_hour
from default.sm_offline_datas  
where log_day = '$datelong' and index = '1' and t = '0' and i = '4' and u is not null and u!='null' and u<>'';

	drop table temp_shopwebdata_2;
	create table temp_shopwebdata_2 as
	select a.uc,a.ru,a.u,a.s,a.new_time,si.ptId,si.stId,si.shopId,si.productId,si.skuId from temp_shopwebdata_1 a
	lateral view getshopinfouat(ru,u,4) si as ptId,stId,shopId,productId,skuId;

drop table temp_shopwebdata_shoptype_3;
create table temp_shopwebdata_shoptype_3 as 
select /*+mapjoin(a)*/ b.* from temp_shoptype a join temp_shopwebdata_2 b on a.stid = b.stid;

drop table temp_shopwebdata_3;
create table temp_shopwebdata_3 as 
select /*+mapjoin(a)*/ b.* from temp_pagetype a join temp_shopwebdata_shoptype_3 b on a.ptId = b.ptId;

drop table temp_shopwebdata_4;
create table temp_shopwebdata_4 as 
select * from 
(select a.uc,a.ptId,a.ru,a.stId,a.s,b.shop_no shopId,a.new_time
from (select * from temp_shopwebdata_3 where shopId == 0) a left outer join temp_shopproduct b
on a.productId = b.product_id
union all
select uc,ptId,ru,stId,s,shopId,new_time from temp_shopwebdata_3 where shopId >= 1) c;

drop table temp_shopwebdata_sesstion_5;
create table temp_shopwebdata_sesstion_5 as 
select shopId,s,min(new_time) orgin_time,count(1) pv,IF(count(1) == 1,1,0) jumps,
(max(unix_timestamp(new_time)) - min(unix_timestamp(new_time))) remains,
sum(IF(stId == 3003,1,0)) productinfo_pv,sum(IF(stId == 1,1,0)) cart_pv,sum(IF(stId == 2,1,0)) order_pv,sum(IF(stId == 3,1,0)) pay_pv
from temp_shopwebdata_4 group by shopId,s;

drop table temp_shopwebdata_sesstion_6;
create table temp_shopwebdata_sesstion_6 as 
select b.shopId,b.s,b.ptId,b.ru,1 visits,1 landing_pv,a.pv,a.jumps,a.remains,a.productinfo_pv,a.cart_pv,a.order_pv,a.pay_pv from
temp_shopwebdata_sesstion_5 a join (select * from temp_shopwebdata_4 where ptId != 3000) b 
on a.shopId = b.shopId and a.s = b.s and a.orgin_time = b.new_time;

drop table temp_shopwebdata_uv_7;
create table temp_shopwebdata_uv_7 as
select shopId,ptId,ru,count(distinct uc) uv from temp_shopwebdata_4 group by shopId,ptId,ru;

drop table temp_shopwebdata_shopInfo_8;
create table temp_shopwebdata_shopInfo_8 as 
select shopId,ptId,ru,sum(landing_pv) landing_pv,sum(pv) pv,sum(productinfo_pv) productinfo_pv,
sum(cart_pv) cart_pv,sum(order_pv) order_pv,sum(pay_pv) pay_pv,sum(visits) visits,sum(jumps) jumps,sum(remains) remains,'' id 
from temp_shopwebdata_sesstion_6 group by shopId,ptId,ru;

drop table temp_shopwebdata_result_9;
create table temp_shopwebdata_result_9 as 
select a.shopId,'$datelong' visit_date,-1 visit_hour,1 data_source
,a.ptId,a.ru,b.uv,a.landing_pv,a.pv,a.productinfo_pv,
a.cart_pv,a.order_pv,a.pay_pv,a.visits,a.jumps,a.remains,'' id
from temp_shopwebdata_shopInfo_8 a join temp_shopwebdata_uv_7 b
on a.shopId = b.shopId and a.ptId = b.ptId and a.ru = b.ru;
"

mysql -h 10.58.47.155 -P 3306 -u root -proot123 -e "delete from data_helper.report_shopassist_sourcepagestat where visit_date = \"$datelong\""
/home/hadoop/sqoop-hive/bin/sqoop-export --connect jdbc:mysql://10.58.47.155:3306/data_helper  --username root --password root123 --table report_shopassist_sourcepagestat --export-dir /user/hive/warehouse/test_shopasis.db/temp_shopwebdata_result_9  --input-null-string "\\\\N" --input-null-non-string "\\\\N" --input-fields-terminated-by "\\01" --input-lines-terminated-by "\\n";
