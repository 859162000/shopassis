
SET mapreduce.job.priority=HIGH;

add jar /home/hadoop/lsj/app_data_assistant2.0/lib/hive_udf.jar;
create temporary function getCenterIDForCategory1ID as 'com.gome.hive.udf_new.getCenterIDForCategory1ID';

use ods_new;

INSERT OVERWRITE TABLE ods_new.app_atg_order partition ( dt = {dt}{hour} , type = 'sm_order' ) 
select 
atg_order.submitted_date,
hour(atg_order.submitted_date) as hour,
atg_order.order_id,
atg_order.product_id,
atg_order.amount,
atg_order.quantity,
atg_order.prd_brand,
atg_order.center_id,
atg_order.category_id_1,
atg_order.category_id_2,
atg_order.category_id_3,
atg_order.user_id,
atg_order.pay_success_flag,
logs.channel_tag,
logs.lpg_type 
from 
(
select 
submitted_date,
order_id,
product_id,
amount,
quantity,
prd_brand,
getCenterIDForCategory1ID(category_id_1) as center_id,
category_id_1,
category_id_2,
category_id_3,
user_id,
pay_success_flag 
from ods_new.app_atg_order 
where dt = {dt}{hour} and type = 'atg_order' 
) atg_order 
inner join 
(
select 
ex_field["orderid"] as order_id,
lpg_type,
channel_tag 
from ods_new.sitemonitor_log 
where dt = {dt}{hour} and type = 'step3_done' and type_id = 52 
) logs 
on atg_order.order_id = logs.order_id;
