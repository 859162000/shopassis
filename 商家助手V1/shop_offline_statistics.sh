#!/bin/bash
cd /home/hadoop/data_helper_new/offine
datelong="$1"
datenow="$2"
/home/hadoop/hive/bin/hive -e "
drop table ods_dmp.date_helper_shop_offline_statistics_tmp_1;
create table ods_dmp.date_helper_shop_offline_statistics_tmp_1 as
select shop_id,
       i,
       count(*) PAGEVIEWS,
       count(distinct uc) USERVIEWS,
       count(distinct uc,visit_ranking) VISITS,
       sum(jump_flag) JUMP_LOSS,
       sum(time_difference) RESIDENCE_TIME      
from ods_dmp.data_helper_Integration
group by shop_id,i having shop_id is not null;

drop table ods_dmp.date_helper_shop_offline_statistics_tmp_2;
create table ods_dmp.date_helper_shop_offline_statistics_tmp_2 as
select tb1.*,if(tb2.shop_id is not null,tb2.NEW_USERVIEWS,0) NEW_USERVIEWS 
from ods_dmp.date_helper_shop_offline_statistics_tmp_1 tb1
left outer join (
select shop_id,i,count(distinct uc) NEW_USERVIEWS 
from ods_dmp.data_helper_Integration
where isNewCooke = 1
group by shop_id,i
)tb2 on tb1.shop_id = tb2.shop_id and tb1.i = tb2.i;

--计算下单量
drop table ods_dmp.data_helper_order_quantity_clean;
create table ods_dmp.data_helper_order_quantity_clean as
select distinct uc,i,orderid,shop_no shop_id from (
select uc,i,orderid,order_prodcut_id from ods_dmp.date_helper_main
where u = 'http://g.gome.com.cn/ec/homeus/cart/cart.jsp'
and order_prodcut_id!='null'
) tb1 inner join ods_dmp.shop_product_rule tb2 
on tb1.order_prodcut_id = tb2.product_id;

drop table ods_dmp.data_helper_order_quantity_statistics;
create table ods_dmp.data_helper_order_quantity_statistics as
select shop_id,i,count(*) orderQuantity 
from ods_dmp.data_helper_order_quantity_clean
group by shop_id,i;

---shop_offline_statistics计算,加下单量
drop table ods_dmp.date_helper_shop_offline_statistics;
create table ods_dmp.date_helper_shop_offline_statistics as
select tmp1.*,if(tmp2.shop_id is not null,orderQuantity,0) orderQuantity
from ods_dmp.date_helper_shop_offline_statistics_tmp_2 tmp1
left outer join ods_dmp.data_helper_order_quantity_statistics tmp2
on tmp1.shop_id = tmp2.shop_id and tmp1.i = tmp2.i;

drop table ods_dmp.date_helper_shop_offline_statistics_import;
create table ods_dmp.date_helper_shop_offline_statistics_import as
select  '' ID,
        shop_id SHOP_ID,
        PAGEVIEWS,
        USERVIEWS,
        VISITS,
        orderQuantity ORDERS,
        RESIDENCE_TIME,
        NEW_USERVIEWS,
        JUMP_LOSS,
        '$datelong' FDATE,
        i DATA_SOURCE,
        '$datenow' UPLOADTIME
from ods_dmp.date_helper_shop_offline_statistics;
"



/home/hadoop/sqoop-hive/bin/sqoop-export --connect jdbc:mysql://10.126.50.115:8306/bigdata  --username ae_prd --password Vja7hwuCH4Qrl4DI --table shop_offline_statistics --export-dir /user/hive/warehouse/ods_dmp.db/date_helper_shop_offline_statistics_import  --input-null-string "\\\\N" --input-null-non-string "\\\\N" --input-fields-terminated-by "\\01" --input-lines-terminated-by "\\n";
