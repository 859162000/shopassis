#!/bin/sh
source /home/hadoop/.bash_profile
source /home/hadoop/lsj/app_data_assistant2.0/conf/configuration.conf


run_path=$__RUN_PATH
bin_path=$run_path/bin
data_path=$run_path/data
log_path=$run_path/log
tmp_path=$run_path/tmp
lib_path=$run_path/lib

log_partition_type=$__LOG_PARTITION_TYPE

now_timestamp=`date +%s`
#now_timestamp=`date -d'2015-5-3 00:00:00' +%s`

if [ $1 ];then
now_timestamp=$1
fi

hour=`date -d@$now_timestamp +%H`
if [ $hour -eq 0 ];then
now_timestamp=`perl -e "print $now_timestamp-3600"`
fi


today=`date -d@$now_timestamp +%Y%m%d`
hour=`date -d@$now_timestamp +%H`

#step3_report_app_date summary
cat $bin_path/step3_report_app_date.sql > $tmp_path/$today-$hour-step3_report_app_date.sql
sed -i 's/{dt}/'$today'/g' $tmp_path/$today-$hour-step3_report_app_date.sql
sed -i 's/{hour}/'$hour'/g' $tmp_path/$today-$hour-step3_report_app_date.sql

#step3_report_app_channel summary
cat $bin_path/step3_report_app_channel.sql > $tmp_path/$today-$hour-step3_report_app_channel.sql
sed -i 's/{dt}/'$today'/g' $tmp_path/$today-$hour-step3_report_app_channel.sql
sed -i 's/{hour}/'$hour'/g' $tmp_path/$today-$hour-step3_report_app_channel.sql

#step3_report_app_center_sale summary
cat $bin_path/step3_report_app_center_sale.sql > $tmp_path/$today-$hour-step3_report_app_center_sale.sql
sed -i 's/{dt}/'$today'/g' $tmp_path/$today-$hour-step3_report_app_center_sale.sql
sed -i 's/{hour}/'$hour'/g' $tmp_path/$today-$hour-step3_report_app_center_sale.sql

#step3_report_app_center_flow summary
cat $bin_path/step3_report_app_center_flow.sql > $tmp_path/$today-$hour-step3_report_app_center_flow.sql
sed -i 's/{dt}/'$today'/g' $tmp_path/$today-$hour-step3_report_app_center_flow.sql
sed -i 's/{hour}/'$hour'/g' $tmp_path/$today-$hour-step3_report_app_center_flow.sql


echo $0'-step3_01 Start:'`date` >> $log_path/$today-$hour-log.log

echo "hive -f $tmp_path/$today-$hour-step3_report_app_date.sql" > $tmp_path/$today-$hour-step3_report_app_date.sh
echo "hive -f $tmp_path/$today-$hour-step3_report_app_channel.sql" > $tmp_path/$today-$hour-step3_report_app_channel.sh
echo "hive -f $tmp_path/$today-$hour-step3_report_app_center_sale.sql" > $tmp_path/$today-$hour-step3_report_app_center_sale.sh
echo "hive -f $tmp_path/$today-$hour-step3_report_app_center_flow.sql" > $tmp_path/$today-$hour-step3_report_app_center_flow.sh

/bin/sh $tmp_path/$today-$hour-step3_report_app_date.sh > $log_path/$today-$hour-step3_report_app_date.log 2>&1 &
/bin/sh $tmp_path/$today-$hour-step3_report_app_channel.sh > $log_path/$today-$hour-step3_report_app_channel.log 2>&1 &
/bin/sh $tmp_path/$today-$hour-step3_report_app_center_sale.sh > $log_path/$today-$hour-step3_report_app_center_sale.log 2>&1 &
/bin/sh $tmp_path/$today-$hour-step3_report_app_center_flow.sh > $log_path/$today-$hour-step3_report_app_center_flow.log 2>&1 &

wait

echo $0'-step3_01 Finish:'`date` >> $log_path/$today-$hour-log.log


#report_app_date
hive -e "
select 
report_date,
report_hour,
pv,
uv,
real_uv,
shop_cart_uv,
submit_uv,
sum_money,
order_num 
from ods_new.report_app_date where dt = "$today$hour" and type = 'all';" > /app/app2.0/$today-$hour-app2.0-report_app_date.tsv

#report_app_channel
hive -e "
select 
report_date,
report_hour,
channel,
dim1,
dim2,
uv,
pv,
real_uv,
order_num,
sum_money,
jump_num,
dim_value 
from ods_new.report_app_channel where dt = "$today$hour" and type = 'all';" > /app/app2.0/$today-$hour-app2.0-report_app_channel.tsv

#report_app_center_sale
hive -e "
SELECT 
report_date,
report_hour,
center_id,
dim1,
dim2,
dim3,
ipv,
online_product_num,
order_num,
order_product_num,
saled_product_num,
sum_money,
uv,
cancel_order_num,
product_click_num,
dim_vale 
FROM ods_new.report_app_center_sale  where dt = "$today$hour" and type = 'all';" > /app/app2.0/$today-$hour-app2.0-report_app_center_sale.tsv

hive -e "
SELECT 
report_date,
report_hour,
center_id,
dim1,
dim2,
dim3,
ipv,
online_product_num,
order_num,
order_product_num,
saled_product_num,
sum_money,
uv,
cancel_order_num,
product_click_num,
dim_vale 
FROM ods_new.report_app_center_sale  where dt = "$today$hour" and type = 'center_c1_op' or type = 'center_c1_c2_op' or type = 'center_c1_c2_c3_op' ;" > /app/app2.0/$today-$hour-app2.0-report_app_center_sale_tmp.tsv




#report_app_center_flow
hive -e "
SELECT 
report_date,
report_hour,
center_id,
dim1,
dim2,
uv,
pv,
exit_num,
visits_num,
sum_access_time,
ipv,
shop_cart_num,
product_click_num,
dim_value 
FROM ods_new.report_app_center_flow where dt = "$today$hour" and type = 'all';" > /app/app2.0/$today-$hour-app2.0-report_app_center_flow.tsv

 


#input hdd fact data to mysql
#report_app_date
mysql -h 10.58.46.17 -P 7306 -u bigdata_bo_rep -peI2YNVCC --default-character-set=utf8 -e "
use data_assistant2;
delete from data_assistant2.report_app_date where report_date = "$today";
load data local infile '/app/app2.0/"$today"-"$hour"-app2.0-report_app_date.tsv' into table data_assistant2.report_app_date CHARACTER SET utf8 
(
report_date,
report_hour,
pv,
uv,
real_uv,
shop_cart_uv,
submit_uv,
sum_money,
order_num 
);"

#report_app_channel
mysql -h 10.58.46.17 -P 7306 -u bigdata_bo_rep -peI2YNVCC --default-character-set=utf8 -e "
use data_assistant2;
delete from data_assistant2.report_app_channel where report_date = "$today";
load data local infile '/app/app2.0/"$today"-"$hour"-app2.0-report_app_channel.tsv' into table data_assistant2.report_app_channel CHARACTER SET utf8 
(
report_date,
report_hour,
channel,
dim1,
dim2,
uv,
pv,
real_uv,
order_num,
sum_money,
jump_num,
dim_value
);"

#report_app_center_sale
mysql -h 10.58.46.17 -P 7306 -u bigdata_bo_rep -peI2YNVCC --default-character-set=utf8 -e "
use data_assistant2;
delete from data_assistant2.report_app_center_sale where report_date = "$today";
load data local infile '/app/app2.0/"$today"-"$hour"-app2.0-report_app_center_sale.tsv' into table data_assistant2.report_app_center_sale CHARACTER SET utf8 
(
report_date,
report_hour,
center_id,
dim1,
dim2,
dim3,
ipv,
online_product_num,
order_num,
order_product_num,
saled_product_num,
sum_money,
uv,
cancel_order_num,
product_click_num,
dim_vale 
);
"

#report_app_center_flow
mysql -h 10.58.46.17 -P 7306 -u bigdata_bo_rep -peI2YNVCC --default-character-set=utf8 -e "
use data_assistant2;
delete from data_assistant2.report_app_center_flow where report_date = "$today";
load data local infile '/app/app2.0/"$today"-"$hour"-app2.0-report_app_center_flow.tsv' into table data_assistant2.report_app_center_flow CHARACTER SET utf8 
(
report_date,
report_hour,
center_id,
dim1,
dim2,
uv,
pv,
exit_num,
visits_num,
sum_access_time,
ipv,
shop_cart_num,
product_click_num,
dim_value 
);"

#output dimension data to hdd
hive -e "select brand_tag,brand_name from tmp_base.dim_brand;" > /app/app2.0/dim_brand.tsv

hive -e "select category_id,category_name,depth,parent_id from tmp_base.dim_category;" > /app/app2.0/dim_category.tsv




#input hdd dimension data to mysql
#mysql -h 10.58.47.155 -u root -proot123 --default-character-set=utf8 -e "
mysql -h 10.58.46.17 -P 7306 -u bigdata_bo_rep -peI2YNVCC --default-character-set=utf8 -e "
use data_assistant;
TRUNCATE TABLE dim_category;
load data local infile '/app/app2.0/dim_category.tsv' into table data_assistant.dim_category CHARACTER SET utf8 (category_id,category_name,depth,parent_id);"




echo $0'-step3 Load MySql Finish:'`date` >> $log_path/$today-$hour-log.log