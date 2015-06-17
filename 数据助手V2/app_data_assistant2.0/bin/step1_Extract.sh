#!/bin/sh
source /home/hadoop/.bash_profile
source /home/hadoop/lsj/app_data_assistant2.0/conf/configuration.conf


run_path=$__RUN_PATH
bin_path=$run_path/bin
data_path=$run_path/data
log_path=$run_path/log
tmp_path=$run_path/tmp
lib_path=$run_path/lib


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


hadoop dfs -rmr /tmp_lsj/DW_CAT_CHLDCAT_2
hadoop dfs -rmr /tmp_lsj/DW_CAT_CHLDPRD_2
hadoop dfs -rmr /tmp_lsj/app_order_2
hadoop dfs -rmr /tmp_lsj/DW_GOME_PRD_BRAND

echo $0'-brand Start:'`date` >> $log_path/$today-$hour-log.log
#Synchronous product brand
sqoop import --connect jdbc:oracle:thin:@10.58.66.37:31521:bodb2 --username 'bigdata_bo_rep' --password 'eI2YNVCC' \
--fields-terminated-by '\t' \
--target-dir /tmp_lsj/DW_GOME_PRD_BRAND \
--outdir /home/hadoop/DW_GOME_PRD_BRAND \
-e "
select product_id,prd_brand,is_sale from DW_GOME_PRD_BRAND 
WHERE \$CONDITIONS" -m 1

echo $0'-DW_CAT_CHLDCAT Start:'`date` >> $log_path/$today-$hour-log.log

#Synchronous category
sqoop import --connect jdbc:oracle:thin:@10.58.66.37:31521:bodb2 --username 'bigdata_bo_rep' --password 'eI2YNVCC' \
--fields-terminated-by '\t' \
--target-dir /tmp_lsj/DW_CAT_CHLDCAT_2 \
--outdir /home/hadoop/DW_CAT_CHLDCAT_2 \
-e "
select * from 
(
select distinct category_id_1,category_id_name_1,1,'0' from DW_CAT_CHLDCAT 
union all 
select distinct category_id_2,category_id_name_2,2,category_id_1 from DW_CAT_CHLDCAT 
union all  
select distinct category_id_3,category_id_name_3,3,category_id_2 from DW_CAT_CHLDCAT 
) tbl 
WHERE \$CONDITIONS" -m 1

echo $0'-DW_CAT_CHLDPRD Start:'`date` >> $log_path/$today-$hour-log.log

#Synchronous category product mapping
sqoop import --connect jdbc:oracle:thin:@10.58.66.37:31521:bodb2 --username 'bigdata_bo_rep' --password 'eI2YNVCC' \
--fields-terminated-by '\t' \
--target-dir /tmp_lsj/DW_CAT_CHLDPRD_2 \
--outdir /home/hadoop/DW_CAT_CHLDPRD_2 \
-e "
select child_prd_id,category_id_1,category_id_2,category_id_3 from DW_CAT_CHLDPRD WHERE \$CONDITIONS" -m 1

echo $0'-atg_order Start:'`date` >> $log_path/$today-$hour-log.log
#Synchronous order
sqoop import --connect jdbc:oracle:thin:@10.58.66.37:31521:bodb2 --username 'bigdata_bo_rep' --password 'eI2YNVCC' \
--fields-terminated-by '\t' \
--target-dir /tmp_lsj/app_order_2 \
--outdir /home/hadoop/app_order_2 \
-e "
select 
* 
from 
(
SELECT 
T.SUBMITTED_DATE,
to_char(T.SUBMITTED_DATE,'hh24'),
T.ORDER_ID,
T.PRODUCT_ID,
SUM(T.AMOUNT) AMOUNT,
SUM(T.QUANTITY) QUANTITY,
T.PRD_BRAND,
0 as center_id,
T.CATEGORY_ID_1,
T.CATEGORY_ID_2,
T.CATEGORY_ID_3,
T.USER_ID,
MAX(T.PAY_SECCESS_FLAG) PAY_SECCESS_FLAG,
'' as channel_tag,
'' as lpg_type 
FROM (SELECT T0.REF_RELATION_ORDER_ID ORDER_ID,
T3.PRODUCT_ID,
DECODE(T4.ON_SALE,
0,
T5.NON_BUNDLE_SALE_PRICE,
T4.SALE_PRICE) * (T2.QUANTITY + T2.RETURNED_QTY) AMOUNT,
DECODE(T4.ON_SALE,
0,
T5.NON_BUNDLE_SALE_PRICE,
T4.SALE_PRICE) PRICE_INFO,
T2.QUANTITY + T2.RETURNED_QTY QUANTITY,
T2.COMMERCE_ITEM_ID,
T1.SHIPPING_GROUP_ID,
T2.RELATIONSHIP_ID,
T6.PRD_BRAND,
NVL(T7.CATEGORY_ID1, T8.CATEGORY_ID1) CATEGORY_ID_1,
NVL(T7.CATEGORY_ID2, T8.CATEGORY_ID2) CATEGORY_ID_2,
NVL(T7.CATEGORY_ID3, T8.CATEGORY_ID3) CATEGORY_ID_3,
T.PROFILE_ID USER_ID,
T.SUBMITTED_DATE,
CASE
WHEN T10.ORDER_ID IS NOT NULL THEN
1
ELSE
0
END PAY_SECCESS_FLAG
FROM (SELECT T.ORDER_ID, T.STATE, T.PROFILE_ID, T.SUBMITTED_DATE
FROM DCSPP_ORDER T
WHERE T.SUBMITTED_DATE >= TO_DATE('"$today" 00:00:00', 'yyyymmdd hh24:mi:ss') 
AND T.SUBMITTED_DATE <= TO_DATE('"$today" "$hour":59:59', 'yyyymmdd hh24:mi:ss') 
AND T.CREATED_BY_ORDER IS NULL --ÕýÏò¶©µ¥
AND T.STATE != 'INVISIBLE') T
JOIN PROD_GG.GOME_ORDER T0
ON T.ORDER_ID = T0.ID
LEFT JOIN PROD_GG.DCSPP_SHIP_GROUP T1
ON T.ORDER_ID = T1.ORDER_REF
LEFT JOIN PROD_GG.DCSPP_SHIPITEM_REL T2
ON T1.SHIPPING_GROUP_ID = T2.SHIPPING_GROUP_ID
JOIN PROD_GG.DCSPP_ITEM T3
ON T2.COMMERCE_ITEM_ID = T3.COMMERCE_ITEM_ID
LEFT JOIN DCSPP_ITEM_PRICE T4
ON T3.PRICE_INFO = T4.AMOUNT_INFO_ID
LEFT JOIN GOME_DCSPP_ITEM_PRICE T5
ON T3.PRICE_INFO = T5.ITEM_PRICE_INFO_ID
LEFT JOIN CATB_OGG.GOME_DCS_PRD_BRAND T6
ON T3.PRODUCT_ID = T6.PRODUCT_ID
LEFT JOIN DW_PRODUCT_CATAGORY_HOME T7
ON T3.PRODUCT_ID = T7.PRODUCT_ID
LEFT JOIN DW_PRODUCT_CATAGORY_COO8 T8
ON T3.PRODUCT_ID = T8.PRODUCT_ID
LEFT JOIN GOME_DCSPP_SHIP_GROUP T9
ON T1.SHIPPING_GROUP_ID = T9.SHIPPING_GROUP_ID
LEFT JOIN (SELECT DISTINCT T10.ORDER_ID ORDER_ID
FROM PROD_GG.GOME_INF_PAYMENT_TASK_LOG T10
WHERE T10.PROCESS_STATUS = 2
AND T10.TRADE_TIME >= TO_DATE('"$today" 00:00:00', 'yyyymmdd hh24:mi:ss') 
AND T10.TRADE_TIME <= TO_DATE('"$today" "$hour":59:59', 'yyyymmdd hh24:mi:ss')) T10
ON T.ORDER_ID = T10.ORDER_ID
WHERE T3.TYPE IN (0, 2)
AND (T7.PRODUCT_ID IS NOT NULL OR
T8.PRODUCT_ID IS NOT NULL)
AND T.STATE NOT IN ('PENDING_CUSTOMER_RETURN', 'REMOVED') 
AND NVL(T9.GOME_STATE, 'AA') NOT IN
('EO', 'UR', 'DA', 'RT', 'RV', 'DH', 'DF', 'CL')) T
GROUP BY T.SUBMITTED_DATE,
to_char(T.SUBMITTED_DATE,'hh24'),
T.ORDER_ID,
T.PRODUCT_ID,
T.PRD_BRAND,
T.CATEGORY_ID_1,
T.CATEGORY_ID_2,
T.CATEGORY_ID_3,
T.USER_ID
) tbl 
WHERE \$CONDITIONS" -m 1




hive -e "LOAD DATA INPATH '/tmp_lsj/DW_CAT_CHLDCAT_2/part*' OVERWRITE INTO TABLE ods_new.dim_category;"
hive -e "LOAD DATA INPATH '/tmp_lsj/DW_CAT_CHLDPRD_2/part*' OVERWRITE INTO TABLE ods_new.mapping_product_categorg;"
hive -e "LOAD DATA INPATH '/tmp_lsj/app_order_2/part*' OVERWRITE INTO TABLE ods_new.app_atg_order partition(dt="$today$hour" , type = 'origin');"
hive -e "LOAD DATA INPATH '/tmp_lsj/DW_GOME_PRD_BRAND/part*' OVERWRITE INTO TABLE ods_new.dim_product_brand;"



#mysql -h 10.58.47.155 -u root -proot123 --default-character-set=utf8 -e "
#use data_assistant;
#load data local infile '/app/"$today"-"$hour"-run_log.tsv' into table data_assistant.run_log; CHARACTER SET utf8 (report_date,hour,log_count);"

#sm log format
cat $bin_path/step1_01_log_format.sql > $tmp_path/$today-$hour-step1_01_log_format.sql
sed -i 's/{dt}/'$today'/g' $tmp_path/$today-$hour-step1_01_log_format.sql
sed -i 's/{hour}/'$hour'/g' $tmp_path/$today-$hour-step1_01_log_format.sql

#order format
cat $bin_path/step1_02_order.sql > $tmp_path/$today-$hour-step1_02_order.sql
sed -i 's/{dt}/'$today'/g' $tmp_path/$today-$hour-step1_02_order.sql
sed -i 's/{hour}/'$hour'/g' $tmp_path/$today-$hour-step1_02_order.sql

echo $0'-step1_01_log_format Start:'`date` >> $log_path/$today-$hour-log.log

echo "hive -f $tmp_path/$today-$hour-step1_01_log_format.sql" > $tmp_path/$today-$hour-step1_01_log_format.sh
/bin/sh $tmp_path/$today-$hour-step1_01_log_format.sh > $log_path/$today-$hour-step1_01_log_format.log 2>&1 

echo $0'-step1_01_log_format Finish:'`date` >> $log_path/$today-$hour-log.log

echo "hive -f $tmp_path/$today-$hour-step1_02_order.sql" > $tmp_path/$today-$hour-step1_02_order.sh
/bin/sh $tmp_path/$today-$hour-step1_02_order.sh > $log_path/$today-$hour-step1_02_order.log 2>&1 

hive -f $bin_path/step1_02_onsale_product.sql 

wait 
echo $0'-step1_02_order Finish:'`date` >> $log_path/$today-$hour-log.log

