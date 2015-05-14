#!/bin/bash
cd /home/hadoop/data_helper_new/offine
datelong="$1"


/home/hadoop/hive/bin/hive -e "
CREATE TEMPORARY FUNCTION getshopproductid AS 'com.gome.udf.GetShopProductID';
CREATE TEMPORARY FUNCTION getshopskuid AS 'com.gome.udf.GetShopSkuID';
CREATE TEMPORARY FUNCTION getshopid AS 'com.gome.udf.GetShopID';
CREATE TEMPORARY FUNCTION getshoplistid AS 'com.gome.udf.GetShopListId';


drop table ods_dmp.shop_product_rule;
create table ods_dmp.shop_product_rule as
select distinct shop.shop_no,sku.product_id  from odsbase.dim_sku sku
inner join default.afcusdw_0_live_catb_0_gome_sku_shop shop
on sku.sku_id = shop.sku_id;

drop table ods_dmp.date_helper_main;
create table ods_dmp.date_helper_main as
select uc,u,ru,ip,i,uid,new_time,log_day,log_hour,orderid,product_id order_prodcut_id from default.sm_offline_datas
where log_day='$datelong' and  index = '1' and  t = '0' and i in('1','2','3');

drop table ods_dmp.date_helper_analyUrl;
create table ods_dmp.date_helper_analyUrl as
select *,if(u RLIKE '^http://www.gome.com.cn/product/.+' or u RLIKE '^http://m.gome.com.cn/product-.+' or u RLIKE '^http://item.gome.com.cn/[a-zA-Z0-9-].+.html',getshopproductid(u),null) product_id,
         if(u RLIKE '^http://www.gome.com.cn/product/.+' or u RLIKE '^http://m.gome.com.cn/product-.+' or u RLIKE '^http://item.gome.com.cn/[a-zA-Z0-9-].+.html',getshopskuid(u),null) sku_id,
         if(u RLIKE '^(http://mall.gome.com.cn/)(.+)/(.*)' or u RLIKE '^http://m.gome.com.cn/shop-.+',getshopid(u),null) shop_no,
         if(u RLIKE '^(http://mall.gome.com.cn/)(.+)(html)(.*)',getshoplistid(u),null) list_id
from ods_dmp.date_helper_main 
where u is not null
and u!='null'
and u<>'';

drop table ods_dmp.date_helper_analyShop_tmp;
create table ods_dmp.date_helper_analyShop_tmp as
select tmp.*,shop.shop_no shop_no_join from ods_dmp.date_helper_analyUrl tmp 
left outer join ods_dmp.shop_product_rule shop
on tmp.product_id = shop.product_id;

drop table ods_dmp.date_helper_analyShop;
create table ods_dmp.date_helper_analyShop as
select uc,u,ru,ip,i,uid,new_time,shop_id,log_day,log_hour from (
select *,if(shop_no is not null,shop_no,shop_no_join) shop_id 
from ods_dmp.date_helper_analyShop_tmp
)tb;

"
