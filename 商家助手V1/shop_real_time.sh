#!/bin/bash
cd /home/hadoop/data_helper_new
flag=`tail -1 /home/hadoop/data_helper_new/real_control`

echo $flag

if [[ $flag = 1 ]];then
     exit 0
fi


echo 1 > /home/hadoop/data_helper_new/real_control
datelong=`date +%Y%m%d`
#datelong=`date +%Y%m%d -d"-1 day"`
datenow=`date "+%Y-%m-%d %H:%M:%S"`
#datelong="$1"
#datenow="$2"
/home/hadoop/hive/bin/hive -e "

CREATE TEMPORARY FUNCTION getshopproductid AS 'com.gome.udf.GetShopProductID';
CREATE TEMPORARY FUNCTION getshopskuid AS 'com.gome.udf.GetShopSkuID';
CREATE TEMPORARY FUNCTION getshopid AS 'com.gome.udf.GetShopID';
CREATE TEMPORARY FUNCTION getshoplistid AS 'com.gome.udf.GetShopListId';

drop table ods_dmp.date_helper_main_real;
create table ods_dmp.date_helper_main_real as 
select uc,u,ru,ip,i,uid,new_time,log_day,log_hour,orderid,product_id order_prodcut_id from default.sm_offline_datas
where log_day='$datelong' and  index = '1' and  t = '0' and i in('1','2','3');

drop table ods_dmp.date_helper_analyUrl_real;
create table ods_dmp.date_helper_analyUrl_real as
select *,if(u RLIKE '^http://www.gome.com.cn/product/.+' or u RLIKE '^http://m.gome.com.cn/product-.+' or u RLIKE '^http://item.gome.com.cn/[a-zA-Z0-9-].+.html',getshopproductid(u),null) product_id,
         if(u RLIKE '^http://www.gome.com.cn/product/.+' or u RLIKE '^http://m.gome.com.cn/product-.+' or u RLIKE '^http://item.gome.com.cn/[a-zA-Z0-9-].+.html',getshopskuid(u),null) sku_id,
         if(u RLIKE '^(http://mall.gome.com.cn/)(.+)/(.*)' or u RLIKE '^http://m.gome.com.cn/shop-.+',getshopid(u),null) shop_no,
         if(u RLIKE '^(http://mall.gome.com.cn/)(.+)(html)(.*)',getshoplistid(u),null) list_id
from ods_dmp.date_helper_main_real 
where u is not null
and u!='null'
and u<>'';

drop table ods_dmp.date_helper_analyShop_tmp_real;
create table ods_dmp.date_helper_analyShop_tmp_real as
select tmp.*,shop.shop_no shop_no_join from ods_dmp.date_helper_analyUrl_real tmp 
left outer join ods_dmp.shop_product_rule shop
on tmp.product_id = shop.product_id;

drop table ods_dmp.date_helper_analyShop_real;
create table ods_dmp.date_helper_analyShop_real as
select uc,u,ru,ip,i,uid,new_time,shop_id,log_day,log_hour from (
select *,if(shop_no is not null,shop_no,shop_no_join) shop_id 
from ods_dmp.date_helper_analyShop_tmp_real
)tb;

drop table ods_dmp.date_helper_shop_real_time;
create table ods_dmp.date_helper_shop_real_time as
select shop_id,i,log_hour,count(*) PAGEVIEWS,count(distinct uc) USERVIEWS from ods_dmp.date_helper_analyShop_real
group by shop_id,i,log_hour having shop_id is not null and log_hour is not null;

drop table ods_dmp.date_helper_shop_real_time_import;
create table ods_dmp.date_helper_shop_real_time_import as
select '' ID,
       SHOP_ID,
       PAGEVIEWS,
       USERVIEWS,
       '$datelong' FDATE_DAY,
       substr(log_hour,9) FDATE_HOUR,
       i DATA_SOURCE,
       '$datenow' UPLOADTIME
       from ods_dmp.date_helper_shop_real_time;
"
echo 0 > /home/hadoop/data_helper_new/real_control
importCount=`hive -e 'select count(*) from ods_dmp.date_helper_shop_real_time_import'`
if [[ $importCount = 0 ]];then
    echo "importCount is 0"
    exit 0
fi

#mysql -h 10.58.47.155 -u root -proot123 -e  "use data_helper;delete from shop_real_time where FDATE_DAY = '$datelong'"
mysql -h 10.126.50.115 -P 8306 -u ae_prd -pVja7hwuCH4Qrl4DI -e "delete from bigdata.shop_real_time where FDATE_DAY = \"$datelong\""

/home/hadoop/sqoop-hive/bin/sqoop-export --connect jdbc:mysql://10.126.50.115:8306/bigdata --username ae_prd --password Vja7hwuCH4Qrl4DI --table shop_real_time --export-dir /user/hive/warehouse/ods_dmp.db/date_helper_shop_real_time_import  --input-null-string "\\\\N" --input-null-non-string "\\\\N" --input-fields-terminated-by "\\01" --input-lines-terminated-by "\\n";

#/home/hadoop/sqoop-hive/bin/sqoop-export --connect jdbc:mysql://10.58.47.155:3306/data_helper_two --username root --password root123 --table shop_real_time --export-dir /user/hive/warehouse/ods_dmp.db/date_helper_shop_real_time_import  --input-null-string "\\\\N" --input-null-non-string "\\\\N" --input-fields-terminated-by "\\01" --input-lines-terminated-by "\\n";
